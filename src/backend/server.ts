import express from "express";
import { podPool } from "../k8s/pod-pool";
import { orchestrator } from "./orchestrator";
import { getWorkflow } from "./workflow-store";

const app = express();
app.use(express.json());

app.post("/workflow", async (req, res) => {
  const workflow = req.body;
  await orchestrator.submitWorkflow(workflow);
  res.status(202).json({
    workflowId: workflow.workflowId,
    status: "accepted",
  });
});

app.get("/workflow/:id", (req, res) => {
  const state = getWorkflow(req.params.id);
  if (!state) {
    return res.status(404).json({ error: "Workflow not found" });
  }
  res.json({
    workflowId: state.workflowId,
    status: state.status,
    steps: state.steps.map((step) => {
      const stepState = state.stepState[step.id];
      return {
        id: step.id,
        status: stepState.status,
        podId: stepState.podId,
        stdout: stepState.stdout,
        exitCode: stepState.exitCode,
        error: stepState.error,
      };
    }),
  });
});

// GET /pods  — already implemented, useful for debugging
app.get("/pods", async (_req, res) => {
  const status = podPool.getPoolStatus();
  res.json(status);
});

export { app };
