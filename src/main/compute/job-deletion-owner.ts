import type { ComputeJob } from '../../shared/compute'
import { sharedDispatchTracker, type DispatchTracker } from './dispatch-tracker'
import { quoteRemotePath, type RemoteHandle } from './job-dispatcher'
import type { ComputeJobOwner, ComputeJobRepository } from './job-repository'
import type { ComputeHostRepository } from './repository'
import { resolveSshTarget, type ResolvedSshTarget, type SshRunner } from './ssh-runner'

type ComputeJobDeletionRepository = Pick<
  ComputeJobRepository,
  'beginOwnerDeletion' | 'abortOwnerDeletion' | 'findByOwner' | 'deleteByOwner'
>

type ComputeJobRuntimePause = {
  pause(): Promise<void>
  resume(): void
}

type ComputeJobDeletionOwnerDeps = {
  jobRepository: ComputeJobDeletionRepository
  hostRepository: Pick<ComputeHostRepository, 'get'>
  runner: SshRunner
  dispatchTracker?: Pick<DispatchTracker, 'waitFor'>
  resolveTarget?: (
    alias: string,
    overrides: Parameters<typeof resolveSshTarget>[1]
  ) => Promise<ResolvedSshTarget>
}

const ACTIVE_STATUSES = new Set<ComputeJob['status']>(['submitted', 'running'])

const validatedRemoteWorkdir = (job: ComputeJob): string => {
  const workdir = job.remote_workdir
  const safeJobId = /^[A-Za-z0-9_-]+$/.test(job.job_id)
  const hasTraversal = workdir?.split('/').some((part) => part === '.' || part === '..')
  if (
    !workdir ||
    !safeJobId ||
    /[\0\r\n]/.test(workdir) ||
    hasTraversal ||
    !workdir.endsWith(`/.openscience/jobs/${job.job_id}`)
  ) {
    throw new Error(`Unsafe remote work directory for Compute Job ${job.job_id}.`)
  }
  return workdir
}

const activeRemoteHandle = (job: ComputeJob, workdir: string): RemoteHandle | undefined => {
  if (!ACTIVE_STATUSES.has(job.status)) return undefined
  if (!job.remote_handle) {
    if (job.status === 'submitted') return undefined
    throw new Error(`Invalid remote handle for active Compute Job ${job.job_id}.`)
  }
  try {
    const handle = JSON.parse(job.remote_handle) as RemoteHandle
    if (!Number.isSafeInteger(handle.pid) || handle.pid <= 0 || handle.workdir !== workdir) {
      throw new Error('invalid handle')
    }
    return handle
  } catch {
    throw new Error(`Invalid remote handle for active Compute Job ${job.job_id}.`)
  }
}

const cleanupCommand = (workdir: string, handle: RemoteHandle | undefined): string => {
  const quotedWorkdir = quoteRemotePath(workdir)
  const quotedPidFile = quoteRemotePath(`${workdir}/job.pid`)
  const lines = handle
    ? [
        `kill -TERM -- -${handle.pid} 2>/dev/null || true`,
        `kill -TERM ${handle.pid} 2>/dev/null || true`,
        `kill -KILL -- -${handle.pid} 2>/dev/null || true`,
        `kill -KILL ${handle.pid} 2>/dev/null || true`
      ]
    : []
  lines.push(
    `if [ -f ${quotedPidFile} ]; then pid=$(cat ${quotedPidFile} 2>/dev/null || true); case $pid in ''|*[!0-9]*) ;; *) kill -TERM -- -$pid 2>/dev/null || true; kill -TERM $pid 2>/dev/null || true; kill -KILL -- -$pid 2>/dev/null || true; kill -KILL $pid 2>/dev/null || true ;; esac; fi`,
    `rm -rf -- ${quotedWorkdir}`,
    `test ! -e ${quotedWorkdir}`
  )
  return lines.join('\n')
}

class ComputeJobDeletionOwner {
  private operationQueue: Promise<unknown> = Promise.resolve()
  private runtime: ComputeJobRuntimePause | undefined
  private readonly dispatchTracker: Pick<DispatchTracker, 'waitFor'>
  private readonly resolveTarget: NonNullable<ComputeJobDeletionOwnerDeps['resolveTarget']>

  constructor(private readonly deps: ComputeJobDeletionOwnerDeps) {
    this.dispatchTracker = deps.dispatchTracker ?? sharedDispatchTracker
    this.resolveTarget = deps.resolveTarget ?? resolveSshTarget
  }

  bindRuntime(runtime: ComputeJobRuntimePause): () => void {
    this.runtime = runtime
    return () => {
      if (this.runtime === runtime) this.runtime = undefined
    }
  }

  deleteSessionJobs(projectId: string, sessionId: string): Promise<void> {
    return this.enqueue({ projectId, sessionId })
  }

  deleteProjectJobs(projectId: string): Promise<void> {
    return this.enqueue({ projectId })
  }

  abortSessionJobDeletion(projectId: string, sessionId: string): Promise<void> {
    return this.enqueueAbort({ projectId, sessionId })
  }

  abortProjectJobDeletion(projectId: string): Promise<void> {
    return this.enqueueAbort({ projectId })
  }

  private enqueue(owner: ComputeJobOwner): Promise<void> {
    const operation = this.operationQueue.then(() => this.deleteOwnerJobs(owner))
    this.operationQueue = operation.catch(() => undefined)
    return operation
  }

  private enqueueAbort(owner: ComputeJobOwner): Promise<void> {
    const operation = this.operationQueue.then(() =>
      this.deps.jobRepository.abortOwnerDeletion(owner)
    )
    this.operationQueue = operation.catch(() => undefined)
    return operation
  }

  private async deleteOwnerJobs(owner: ComputeJobOwner): Promise<void> {
    await this.deps.jobRepository.beginOwnerDeletion(owner)
    let paused = false
    try {
      if (this.runtime) {
        await this.runtime.pause()
        paused = true
      }
      const observed = await this.deps.jobRepository.findByOwner(owner)
      await this.dispatchTracker.waitFor(observed.map((job) => job.job_id))
      const jobs = await this.deps.jobRepository.findByOwner(owner)
      for (const job of jobs) await this.cleanupRemoteJob(job)
      await this.deps.jobRepository.deleteByOwner(owner)
    } catch (error) {
      await this.deps.jobRepository.abortOwnerDeletion(owner)
      throw error
    } finally {
      if (paused) this.runtime?.resume()
    }
  }

  private async cleanupRemoteJob(job: ComputeJob): Promise<void> {
    if (job.status === 'queued') return
    const workdir = validatedRemoteWorkdir(job)
    const handle = activeRemoteHandle(job, workdir)
    const host = await this.deps.hostRepository.get(job.provider_id)
    if (!host) {
      throw new Error(`Cannot clean Compute Job ${job.job_id} because its host no longer exists.`)
    }
    const target = await this.resolveTarget(host.sshAlias, host.sshOverrides)
    const result = await this.deps.runner.run(target, cleanupCommand(workdir, handle), {
      timeoutMs: 30_000,
      loginShell: false,
      maxOutputBytes: 4 * 1024
    })
    if (result.timedOut || result.exitCode !== 0) {
      const detail = result.stderr.trim() || `exit ${result.exitCode ?? 'null'}`
      throw new Error(`Remote Compute Job cleanup failed for ${job.job_id}: ${detail}`)
    }
  }
}

export { ComputeJobDeletionOwner, cleanupCommand, validatedRemoteWorkdir }
export type { ComputeJobDeletionOwnerDeps, ComputeJobDeletionRepository, ComputeJobRuntimePause }
