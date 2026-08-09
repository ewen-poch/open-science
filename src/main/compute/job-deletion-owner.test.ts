import { describe, expect, it, vi } from 'vitest'

import type { ComputeHost, ComputeJob } from '../../shared/compute'
import { cleanupCommand, ComputeJobDeletionOwner } from './job-deletion-owner'
import type { ResolvedSshTarget, SshRunner } from './ssh-runner'

const target: ResolvedSshTarget = { sshBinary: 'ssh', host: 'cluster', extraArgs: [] }
const host = {
  id: 'host-1',
  providerId: 'ssh:cluster',
  displayName: 'Cluster',
  shape: 'direct_ssh',
  sshAlias: 'cluster',
  createdAt: 1,
  updatedAt: 1
} as ComputeHost

const job = (overrides: Partial<ComputeJob> = {}): ComputeJob => ({
  job_id: 'job-1',
  provider_id: host.providerId,
  project_id: 'project-1',
  session_id: 'session-1',
  shape: 'direct_ssh',
  status: 'running',
  intent: 'analysis',
  command: 'sleep 600',
  command_hash: 'hash',
  environment: undefined,
  resource_request: undefined,
  input_manifest: undefined,
  output_manifest: undefined,
  harvest_config: undefined,
  timeout_seconds: 600,
  remote_workdir: '~/.openscience/jobs/job-1',
  remote_handle: JSON.stringify({
    pid: 123,
    exit_code_path: '~/.openscience/jobs/job-1/exit_code',
    stdout_path: '~/.openscience/jobs/job-1/stdout',
    stderr_path: '~/.openscience/jobs/job-1/stderr',
    workdir: '~/.openscience/jobs/job-1'
  }),
  exit_code: undefined,
  stdout_tail: undefined,
  stderr_tail: undefined,
  error_code: undefined,
  created_at: 1,
  submitted_at: 1,
  started_at: 1,
  finished_at: undefined,
  harvested_at: undefined,
  ...overrides
})

// Preserve the inferred Vitest mock types so individual tests can configure failures without casts.
// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
const createHarness = (jobs: ComputeJob[]) => {
  const order: string[] = []
  const jobRepository = {
    beginOwnerDeletion: vi.fn(async () => {
      order.push('begin')
    }),
    abortOwnerDeletion: vi.fn(async () => {
      order.push('abort')
    }),
    findByOwner: vi.fn(async () => jobs),
    deleteByOwner: vi.fn(async () => {
      order.push('delete-rows')
    })
  }
  const hostRepository = { get: vi.fn(async () => host) }
  const runner = {
    run: vi.fn(async () => {
      order.push('remote-cleanup')
      return {
        exitCode: 0,
        stdout: '',
        stderr: '',
        truncated: false,
        timedOut: false
      }
    })
  } satisfies SshRunner
  const dispatchTracker = {
    waitFor: vi.fn(async () => {
      order.push('dispatch-drained')
    })
  }
  const runtime = {
    pause: vi.fn(async () => {
      order.push('poller-paused')
    }),
    resume: vi.fn(() => {
      order.push('poller-resumed')
    })
  }
  const owner = new ComputeJobDeletionOwner({
    jobRepository,
    hostRepository,
    runner,
    dispatchTracker,
    resolveTarget: vi.fn(async () => target)
  })
  owner.bindRuntime(runtime)
  return {
    owner,
    order,
    jobRepository,
    hostRepository,
    runner,
    dispatchTracker,
    runtime
  }
}

describe('ComputeJobDeletionOwner', () => {
  it('kills the process group and removes the generated directory', () => {
    const rawHandle = job().remote_handle ?? ''
    const handle = JSON.parse(rawHandle) as Parameters<typeof cleanupCommand>[1]
    const command = cleanupCommand('~/.openscience/jobs/job-1', handle)
    expect(command).toContain('kill -TERM -- -123')
    expect(command).toContain('job.pid')
    expect(command).toContain('rm -rf --')
    expect(command).toContain('test ! -e')
  })

  it('terminates an active Session job before deleting its row', async () => {
    const harness = createHarness([job()])
    await harness.owner.deleteSessionJobs('project-1', 'session-1')
    expect(harness.dispatchTracker.waitFor).toHaveBeenCalledWith(['job-1'])
    expect(harness.runner.run).toHaveBeenCalledOnce()
    expect(harness.jobRepository.deleteByOwner).toHaveBeenCalledWith({
      projectId: 'project-1',
      sessionId: 'session-1'
    })
    expect(harness.order).toEqual([
      'begin',
      'poller-paused',
      'dispatch-drained',
      'remote-cleanup',
      'delete-rows',
      'poller-resumed'
    ])
  })

  it('deletes queued jobs without contacting the remote host', async () => {
    const harness = createHarness([job({ status: 'queued', remote_handle: undefined })])
    await harness.owner.deleteSessionJobs('project-1', 'session-1')
    expect(harness.hostRepository.get).not.toHaveBeenCalled()
    expect(harness.runner.run).not.toHaveBeenCalled()
    expect(harness.jobRepository.deleteByOwner).toHaveBeenCalledOnce()
  })

  it('removes terminal Job directories during Project deletion', async () => {
    const harness = createHarness([
      job({ status: 'success', remote_handle: undefined, notified_at: 10 })
    ])
    await harness.owner.deleteProjectJobs('project-1')
    expect(harness.runner.run).toHaveBeenCalledOnce()
    expect(harness.jobRepository.deleteByOwner).toHaveBeenCalledWith({
      projectId: 'project-1'
    })
  })

  it('fails closed and restores admission when remote cleanup fails', async () => {
    const harness = createHarness([job()])
    harness.runner.run.mockResolvedValueOnce({
      exitCode: 255,
      stdout: '',
      stderr: 'offline',
      truncated: false,
      timedOut: false
    })
    await expect(harness.owner.deleteSessionJobs('project-1', 'session-1')).rejects.toThrow(
      /remote Compute Job cleanup failed/i
    )
    expect(harness.jobRepository.deleteByOwner).not.toHaveBeenCalled()
    expect(harness.jobRepository.abortOwnerDeletion).toHaveBeenCalledOnce()
    expect(harness.runtime.resume).toHaveBeenCalledOnce()
  })

  it('rejects a remote directory outside the generated Job directory', async () => {
    const harness = createHarness([
      job({ remote_workdir: '/scratch/unrelated', remote_handle: undefined })
    ])
    await expect(harness.owner.deleteProjectJobs('project-1')).rejects.toThrow(
      /unsafe remote work directory/i
    )
    expect(harness.runner.run).not.toHaveBeenCalled()
    expect(harness.jobRepository.deleteByOwner).not.toHaveBeenCalled()
  })

  it('rejects Windows-style work directories because SSH cleanup uses POSIX paths', async () => {
    const harness = createHarness([
      job({
        status: 'success',
        remote_workdir: String.raw`C:\Users\scientist\.openscience\jobs\job-1`,
        remote_handle: undefined
      })
    ])
    await expect(harness.owner.deleteProjectJobs('project-1')).rejects.toThrow(
      /unsafe remote work directory/i
    )
    expect(harness.runner.run).not.toHaveBeenCalled()
    expect(harness.jobRepository.deleteByOwner).not.toHaveBeenCalled()
  })

  it('rejects a malformed active remote handle', async () => {
    const harness = createHarness([job({ remote_handle: '{bad json' })])
    await expect(harness.owner.deleteSessionJobs('project-1', 'session-1')).rejects.toThrow(
      /invalid remote handle/i
    )
    expect(harness.runner.run).not.toHaveBeenCalled()
    expect(harness.jobRepository.deleteByOwner).not.toHaveBeenCalled()
  })
})
