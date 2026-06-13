import type {
  StepState,
  StepStatus,
  Workflow,
  WorkflowState,
  WorkflowStatus,
} from "../types/workflow";
import { getWorkflow, setWorkflow } from "./workflow-store";
import { getReadySteps } from "./dag";
import { stepQueue } from "../queue/step-queue";
import { resultQueue } from "../queue/result-queue";
import { podManager } from "../pod-manager/pod-manager";

/**
 * TODO: Implement this class.
 *
 * The orchestrator is the brain of the system. It ties together
 * the DAG resolver, step queue, result queue, and pod manager.
 *
 * submitWorkflow(workflow):
 *   1. Store workflow in workflow-store with all steps as PENDING
 *   2. Run getReadySteps() to find immediately runnable steps
 *   3. Push each ready step into the step queue
 *   4. Mark those steps as QUEUED in workflow-store
 *
 * IMPORTANT: Store the workflow state before enqueueing the first ready step.
 * Result events can come back quickly, and the result handler must be able
 * to find the workflow in workflow-store.
 *
 * start():
 *   - Begin consuming from the result queue
 *   - Begin draining the step queue (send steps to pod manager)
 *   - For Section 1: sequential is fine
 *   - For Section 2: run parallel dispatch
 *
 * On StepResult received (from result queue consumer):
 *   1. Update the step's status in workflow-store
 *   2. If COMPLETED: run getReadySteps() again, enqueue newly unblocked steps
 *   3. Check if all steps done → update workflow status to "completed" or "failed"
 *
 * INVARIANT: Backend is the ONLY place that writes stepStatus.
 * Pod manager only pushes events. Orchestrator reads events and updates state.
 */
export class Orchestrator {
  private async enqueueReadySteps(workflowId: string): Promise<void> {
    const state = getWorkflow(workflowId);
    if (!state) return;
    const stepStatus: Record<string, StepStatus> = {};
    for (const [stepId, s] of Object.entries(state.stepState)) {
      stepStatus[stepId] = s.status;
    }
    const readySteps = getReadySteps(state.steps, stepStatus);
    for (const step of readySteps) {
      await stepQueue.enqueue({
        stepId: step.id,
        workflowId,
        command: step.command,
        enqueuedAt: Date.now(),
      });
      state.stepState[step.id].status = "QUEUED";
    }
    if (readySteps.length > 0) {
      state.status = "running";
    }
    setWorkflow(workflowId, state);
  }

  private isProcessing = false;

  private async processNextStep(): Promise<void> {
    if (this.isProcessing) return;
    this.isProcessing = true;

    try {
      const step = await stepQueue.dequeue();
      if (!step) return;

      await podManager.dispatch(step);
    } finally {
      this.isProcessing = false;

      if ((await stepQueue.size()) > 0) {
        this.processNextStep();
      }
    }
  }

  async submitWorkflow(workflow: Workflow): Promise<void> {
    // 1. Store the workflow in memory (`workflow-store.ts`)
    // 2. Mark all steps as `PENDING`
    const stepState: Record<string, StepState> = {};
    for (const step of workflow.steps) {
      stepState[step.id] = {
        stepId: step.id,
        status: "PENDING",
        podId: null,
        retriesLeft: step.retries,
      };
    }
    const state: WorkflowState = {
      workflowId: workflow.workflowId,
      status: "pending",
      steps: workflow.steps,
      stepState,
    };
    setWorkflow(workflow.workflowId, state);

    // 1. Run `getReadySteps()` to find immediately runnable steps
    // 2. Push them into the step queue
    // 3. Mark those steps as `QUEUED`
    // 4. Return `{ workflowId, status: "accepted" }` immediately
    // const stepStatus: Record<string, StepStatus> = {};
    // for (const [stepId, s] of Object.entries(stepState)) {
    //   stepStatus[stepId] = s.status;
    // }
    // const readySteps = getReadySteps(workflow.steps, stepStatus);

    // for (const step of readySteps) {
    //   await stepQueue.enqueue({
    //     stepId: step.id,
    //     workflowId: workflow.workflowId,
    //     command: step.command,
    //     enqueuedAt: Date.now(),
    //   });
    //   state.stepState[step.id].status = "QUEUED";
    // }
    // if (readySteps.length > 0) {
    //   state.status = "running";
    // }
    // setWorkflow(workflow.workflowId, state);
    await this.enqueueReadySteps(workflow.workflowId);
    this.processNextStep();

    // void workflow;
    // void getWorkflow;
    // void setWorkflow;
    // void getReadySteps;
    // void stepQueue;
    // throw new Error("TODO: implement submitWorkflow");
  }

  async start(): Promise<void> {
    //     On startup:
    //   Start consuming from result queue
    // On StepResult received:
    //   1. Update step status in workflow-store
    //   2. If status is COMPLETED:
    //        - Re-run getReadySteps()
    //        - Push newly unblocked steps into step queue
    //        - Mark them QUEUED
    //   3. Check if all steps are COMPLETED → mark workflow as "completed"
    // On step dequeued from step queue:
    //   1. Send step to pod manager for execution

    //step-1: status fo step and workflow
    resultQueue.consume(async (result) => {
      const state = getWorkflow(result.workflowId);
      if (!state) return;
      const stepState = state?.stepState[result.stepId];
      if (!stepState) return;
      if (result.status === "RUNNING") {
        stepState.status = "RUNNING";
        stepState.podId = result.podId;
      }
      if (result.status === "COMPLETED") {
        stepState.status = "COMPLETED";
        stepState.podId = result.podId;
        stepState.stdout = result.stdout;
        stepState.exitCode = result.exitCode;
        // for next step
        this.enqueueReadySteps(result.workflowId);
        this.processNextStep();
      }
      if (result.status === "FAILED") {
        stepState.status = "FAILED";
        stepState.podId = result.podId;
        stepState.error = result.error;
      }

      let workFlowStatus = "running";
      const statuses = Object.values(state.stepState).map((s) => s.status);
      if (statuses.some((s) => s === "FAILED")) workFlowStatus = "failed";
      if (statuses.every((s) => s === "COMPLETED"))
        workFlowStatus = "completed";
      if (statuses.some((s) => s === "RUNNING" || s === "QUEUED"))
        workFlowStatus = "running";
      if (statuses.every((s) => s === "PENDING")) workFlowStatus = "pending";
      state.status = workFlowStatus as WorkflowStatus;
      setWorkflow(result.workflowId, state);
    });
  }
}

export const orchestrator = new Orchestrator();
