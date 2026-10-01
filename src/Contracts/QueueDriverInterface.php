<?php

declare(strict_types=1);

namespace EidCloud\JobEngine\Contracts;

use EidCloud\JobEngine\Models\JobPayload;

interface QueueDriverInterface
{
    /**
     * Push a job payload into the specified queue.
     */
    public function push(JobPayload $job): string;

    /**
     * Pop the next available job payload from the specified queue(s).
     *
     * @param string[]|string $queues
     */
    public function pop(array|string $queues): ?JobPayload;

    /**
     * Acknowledge that a job has finished successfully.
     */
    public function acknowledge(string $jobId): bool;

    /**
     * Release a failed or unacknowledged job back into the queue with optional delay.
     */
    public function release(JobPayload $job, int $delaySeconds = 0): bool;

    /**
     * Move a permanently failed job into the Dead Letter Queue (DLQ).
     */
    public function moveToDeadLetter(JobPayload $job, string $reason = ''): bool;

    /**
     * Retrieve all or paginated jobs in the Dead Letter Queue.
     *
     * @return array<int, array<string, mixed>>
     */
    public function getDeadLetters(int $limit = 50, int $offset = 0): array;

    /**
     * Replay a job from DLQ back into its original queue.
     */
    public function replayDeadLetter(string $jobId): bool;

    /**
     * Purge dead letters.
     */
    public function purgeDeadLetter(?string $jobId = null): int;

    /**
     * Count pending jobs in the queue(s).
     *
     * @param string[]|string|null $queues
     */
    public function size(array|string|null $queues = null): int;

    /**
     * Get queue statistics (counts for pending, processing, delayed, failed/dlq, completed).
     *
     * @return array<string, int>
     */
    public function stats(): array;

    /**
     * Purge/clear all jobs from the queue.
     */
    public function clear(?string $queue = null): void;
}
