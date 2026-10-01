<?php

declare(strict_types=1);

namespace EidCloud\JobEngine;

use EidCloud\JobEngine\Contracts\QueueDriverInterface;
use EidCloud\JobEngine\Drivers\SqliteQueueDriver;
use EidCloud\JobEngine\Drivers\FileQueueDriver;
use EidCloud\JobEngine\Drivers\RedisQueueDriver;
use EidCloud\JobEngine\Models\JobPayload;
use EidCloud\JobEngine\Worker\Worker;
use EidCloud\JobEngine\Worker\WorkerOptions;
use EidCloud\JobEngine\Worker\ProcessPool;
use InvalidArgumentException;

class JobQueue
{
    private QueueDriverInterface $driver;

    public function __construct(QueueDriverInterface|string $driver = 'sqlite', array $config = [])
    {
        if (is_string($driver)) {
            $this->driver = self::createDriver($driver, $config);
        } else {
            $this->driver = $driver;
        }
    }

    public static function createDriver(string $name, array $config = []): QueueDriverInterface
    {
        return match (strtolower($name)) {
            'sqlite' => new SqliteQueueDriver($config['path'] ?? sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'eidcloud_jobs.sqlite'),
            'file' => new FileQueueDriver($config['path'] ?? sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'eidcloud_jobs_store'),
            'redis' => new RedisQueueDriver($config['connection'] ?? $config),
            default => throw new InvalidArgumentException("Unsupported queue driver: {$name}"),
        };
    }

    public function getDriver(): QueueDriverInterface
    {
        return $this->driver;
    }

    /**
     * Dispatch a job into the queue.
     *
     * @param string $jobClass FQCN of the job class
     * @param array<string, mixed> $data Input payload
     * @param string $queue Queue name (e.g. 'default', 'high', 'media')
     * @param int $priority Priority score (higher executed first)
     * @param int $delaySeconds Delay before job becomes available
     * @param int $maxAttempts Maximum retry attempts
     * @param int $timeout Timeout in seconds
     * @param string|null $rateLimitKey Rate limit bucket key
     * @param array $chain Chained jobs to execute upon success
     * @return string Dispatched Job ID
     */
    public function dispatch(
        string $jobClass,
        array $data = [],
        string $queue = 'default',
        int $priority = JobPayload::PRIORITY_DEFAULT,
        int $delaySeconds = 0,
        int $maxAttempts = 3,
        int $timeout = 60,
        ?string $rateLimitKey = null,
        array $chain = []
    ): string {
        $availableAt = time() + max(0, $delaySeconds);

        $payload = new JobPayload(
            jobClass: $jobClass,
            data: $data,
            queue: $queue,
            priority: $priority,
            maxAttempts: $maxAttempts,
            timeout: $timeout,
            chain: $chain,
            rateLimitKey: $rateLimitKey,
            availableAt: $availableAt
        );

        return $this->driver->push($payload);
    }

    /**
     * Dispatch a pre-configured JobPayload instance.
     */
    public function dispatchPayload(JobPayload $payload): string
    {
        return $this->driver->push($payload);
    }

    /**
     * Run a single worker or return a Worker instance.
     */
    public function worker(array|string $queues = ['default'], ?WorkerOptions $options = null): Worker
    {
        return new Worker($this->driver, $queues, $options);
    }

    /**
     * Run a multi-process worker pool.
     */
    public function pool(array|string $queues = ['default'], int $workers = 4, ?WorkerOptions $options = null): ProcessPool
    {
        return new ProcessPool($this->driver, $queues, $workers, $options);
    }

    /**
     * Queue size.
     */
    public function size(array|string|null $queues = null): int
    {
        return $this->driver->size($queues);
    }

    /**
     * Queue stats.
     */
    public function stats(): array
    {
        return $this->driver->stats();
    }

    /**
     * Dead letter queue records.
     */
    public function dlq(int $limit = 50, int $offset = 0): array
    {
        return $this->driver->getDeadLetters($limit, $offset);
    }

    /**
     * Replay a failed job from DLQ.
     */
    public function replay(string $jobId): bool
    {
        return $this->driver->replayDeadLetter($jobId);
    }

    /**
     * Purge dead letters.
     */
    public function purgeDlq(?string $jobId = null): int
    {
        return $this->driver->purgeDeadLetter($jobId);
    }

    /**
     * Clear queue.
     */
    public function clear(?string $queue = null): void
    {
        $this->driver->clear($queue);
    }
}
