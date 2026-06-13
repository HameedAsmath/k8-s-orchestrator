import type { QueuedStep } from "../types/workflow"
import { redis } from "./redis-client"

const STEP_QUEUE_KEY = "workflow:step-queue"

function parseQueuedStep(value: string): QueuedStep {
  return JSON.parse(value) as QueuedStep
}

/**
 * A fully implemented FIFO queue backed by Redis.
 *
 * Students should use this queue from the orchestrator instead of
 * reimplementing Redis list operations.
 *
 * Use LPUSH to enqueue and RPOP to dequeue.
 * Items are serialized as JSON strings in Redis.
 */
export class StepQueue {
  async enqueue(step: QueuedStep): Promise<void> {
    await redis.lpush(STEP_QUEUE_KEY, JSON.stringify(step))
  }

  async dequeue(): Promise<QueuedStep | null> {
    const value = await redis.rpop(STEP_QUEUE_KEY)
    if (!value) return null

    return parseQueuedStep(value)
  }

  async peek(): Promise<QueuedStep | null> {
    const values = await redis.lrange(STEP_QUEUE_KEY, -1, -1)
    const value = values[0]
    if (!value) return null

    return parseQueuedStep(value)
  }

  async size(): Promise<number> {
    return redis.llen(STEP_QUEUE_KEY)
  }
}

export const stepQueue = new StepQueue()
