import type { QueuedStep } from "../types/workflow";
import { podPool } from "../k8s/pod-pool";
import { resultQueue } from "../queue/result-queue";

/**
 * TODO: Implement this class.
 *
 * The pod manager receives a step, runs it inside a leased pod,
 * and publishes status events to the result queue.
 *
 * dispatch(step) must:
 *   1. Call podPool.acquirePod() to lease a free pod
 *   2. Push { status: "RUNNING", podId, stepId, workflowId } to resultQueue
 *   3. Call podPool.execInPod(podId, step.command)
 *   4. Push { status: "COMPLETED", stdout, exitCode: 0 } to resultQueue
 *   5. Call podPool.releasePod(podId)
 *
 *   On any error:
 *   4. Push { status: "FAILED", error: err.message } to resultQueue
 *   5. Call podPool.releasePod(podId)  ← always release in finally block
 *
 * IMPORTANT: Pod manager never reads or writes workflow state.
 * It only pushes to the result queue.
 */
export class PodManager {
  async dispatch(step: QueuedStep): Promise<void> {
    // 1. acquirePod()                          ← from boilerplate
    // 2. publish { status: "RUNNING" }         → result queue
    // 3. execInPod(podId, command)             ← from boilerplate
    // 4. publish { status: "COMPLETED" }       → result queue
    // 5. releasePod(podId)                     ← from boilerplate
    let podId: string | null = null;

    try {
      const pod = await podPool.acquirePod();
      podId = pod.podId;
      await resultQueue.push({
        stepId: step.stepId,
        workflowId: step.workflowId,
        podId,
        status: "RUNNING",
      });
      const output = await podPool.execInPod(podId, step.command);
      await resultQueue.push({
        stepId: step.stepId,
        workflowId: step.workflowId,
        podId,
        status: "COMPLETED",
        stdout: output,
      });
    } catch (error) {
      if (podId) {
        await resultQueue.push({
          stepId: step.stepId,
          workflowId: step.workflowId,
          podId,
          status: "FAILED",
          error: String(error),
        });
      }
    } finally {
      if (podId) {
        await podPool.releasePod(podId);
      }
    }
  }
}

export const podManager = new PodManager();
