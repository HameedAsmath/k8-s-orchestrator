import { describe, expect, test } from "bun:test";
import { getReadySteps } from "../backend/dag";

describe("getReadySteps", () => {
  const steps = [
    { id: "A", command: "echo A" },
    { id: "B", command: "echo B", dependsOn: ["A"] },
    { id: "C", command: "echo C", dependsOn: ["A"] },
    { id: "D", command: "echo D", dependsOn: ["B", "C"] },
  ];

  test("returns root steps when all are PENDING", () => {
    const ready = getReadySteps(steps, {
      A: "PENDING",
      B: "PENDING",
      C: "PENDING",
      D: "PENDING",
    });
    expect(ready.map((s) => s.id)).toEqual(["A"]);
  });

  test("returns B and C after A completes", () => {
    const ready = getReadySteps(steps, {
      A: "COMPLETED",
      B: "PENDING",
      C: "PENDING",
      D: "PENDING",
    });
    expect(ready.map((s) => s.id)).toEqual(["B", "C"]);
  });

  test("does not return already QUEUED or COMPLETED steps", () => {
    const ready = getReadySteps(steps, {
      A: "COMPLETED",
      B: "QUEUED",
      C: "COMPLETED",
      D: "PENDING",
    });
    expect(ready.map((s) => s.id)).toEqual([]);
  });
});
