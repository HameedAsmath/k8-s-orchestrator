import type { StepResult } from "../types/workflow"
import { redis } from "./redis-client"

const RESULT_QUEUE_KEY = "workflow:result-queue"

function parseStepResult(value: string): StepResult {
  return JSON.parse(value) as StepResult
}

/**
 * A Redis-backed result queue for events coming back from the pod manager.
 *
 * Redis plumbing is provided so students can focus on orchestrator behavior.
 * The blocking BRPOP consumer uses a dedicated Redis connection.
 */
export class ResultQueue {
  async push(result: StepResult): Promise<void> {
    await redis.lpush(RESULT_QUEUE_KEY, JSON.stringify(result))
  }

  async consume(handler: (result: StepResult) => Promise<void>): Promise<void> {
    const subscriber = redis.duplicate()

    while (true) {
      const item = await subscriber.brpop(RESULT_QUEUE_KEY, 0)
      if (!item) continue

      const [, value] = item
      const result = parseStepResult(value)

      // TODO: Students implement result handling in orchestrator.ts by passing a handler here.
      await handler(result)
    }
  }
}

export const resultQueue = new ResultQueue()
