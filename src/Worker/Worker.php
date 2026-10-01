<?php

declare(strict_types=1);

namespace EidCloud\JobEngine\Worker;

use EidCloud\JobEngine\Contracts\QueueDriverInterface;
use EidCloud\JobEngine\Models\JobPayload;
use EidCloud\JobEngine\Exceptions\JobTimeoutException;
use Throwable;

class Worker
{
    private QueueDriverInterface $driver;
    private array $queues;
    private WorkerOptions $options;
    private bool $shouldStop = false;
    private string $id;
    private ?string $heartbeatFile;
    private int $processedCount = 0;
    private int $failedCount = 0;

    /**
     * @var array<string, int> Tracks timestamp of last execution per rate-limit key
     */
    private static array $rateLimitHistory = [];

    public function __construct(
        QueueDriverInterface $driver,
        array|string $queues = ['default'],
        ?WorkerOptions $options = null,
        ?string $id = null,
        ?string $heartbeatFile = null
    ) {
        $this->driver = $driver;
        $this->queues = (array)$queues;
        $this->options = $options ?? new WorkerOptions();
        $this->id = $id ?? 'worker-' . bin2hex(random_bytes(4));
        $this->heartbeatFile = $heartbeatFile;

        $this->registerSignalHandlers();
    }

    private function registerSignalHandlers(): void
    {
        if (function_exists('pcntl_signal')) {
            pcntl_async_signals(true);
            pcntl_signal(SIGTERM, fn() => $this->stop());
            pcntl_signal(SIGINT, fn() => $this->stop());
            pcntl_signal(SIGQUIT, fn() => $this->stop());
        }
    }

    public function getId(): string
    {
        return $this->id;
    }

    public function stop(): void
    {
        $this->shouldStop = true;
    }

    public function shouldStop(): bool
    {
        return $this->shouldStop;
    }

    public function beat(): void
    {
        if ($this->heartbeatFile) {
            $data = [
                'id' => $this->id,
                'pid' => getmypid(),
                'time' => time(),
                'processed' => $this->processedCount,
                'failed' => $this->failedCount,
                'queues' => $this->queues,
            ];
            @file_put_contents($this->heartbeatFile, json_encode($data));
        }
    }

    /**
     * Start worker loop until stopped or max jobs reached.
     */
    public function work(): void
    {
        $this->beat();

        while (!$this->shouldStop) {
            $this->beat();

            $job = $this->driver->pop($this->queues);

            if (!$job) {
                if ($this->options->stopWhenEmpty) {
                    break;
                }
                usleep($this->options->sleepMicroseconds);
                continue;
            }

            // Check rate limiting
            if ($job->getRateLimitKey()) {
                $now = time();
                $lastRun = self::$rateLimitHistory[$job->getRateLimitKey()] ?? 0;
                $delayRequired = $this->options->rateLimitDelaySeconds;

                if (($now - $lastRun) < $delayRequired) {
                    // Release back with remaining delay
                    $releaseDelay = $delayRequired - ($now - $lastRun);
                    $this->driver->release($job, max(1, $releaseDelay));
                    usleep(100000);
                    continue;
                }

                self::$rateLimitHistory[$job->getRateLimitKey()] = $now;
            }

            $this->processJob($job);

            if ($this->options->maxJobs > 0 && ($this->processedCount + $this->failedCount) >= $this->options->maxJobs) {
                break;
            }

            if ($this->options->maxMemoryBytes > 0 && memory_get_usage(true) >= $this->options->maxMemoryBytes) {
                break;
            }
        }
    }

    /**
     * Process a single job payload.
     */
    public function processJob(JobPayload $job): bool
    {
        $startTime = microtime(true);

        try {
            $this->executeJob($job);

            // Job succeeded! Acknowledge
            $this->driver->acknowledge($job->getId());
            $this->processedCount++;

            // Handle job chaining
            if ($job->hasChain()) {
                $nextJob = $job->shiftNextChainedJob();
                if ($nextJob) {
                    $this->driver->push($nextJob);
                }
            }

            return true;
        } catch (Throwable $e) {
            $this->failedCount++;
            $job->setLastError($e->getMessage() . "\n" . $e->getTraceAsString());

            if ($job->canRetry()) {
                // Exponential backoff or standard delay
                $delay = $job->calculateNextRetryDelay();
                $this->driver->release($job, $delay);
            } else {
                // Permanent failure -> Dead Letter Queue
                $this->driver->moveToDeadLetter($job, $e->getMessage());
            }

            return false;
        }
    }

    private function executeJob(JobPayload $job): mixed
    {
        $jobClass = $job->getJobClass();

        if (!class_exists($jobClass)) {
            throw new \RuntimeException("Job class '{$jobClass}' not found.");
        }

        $instance = new $jobClass();

        // Enforce timeout using pcntl_alarm where available
        $hasAlarm = function_exists('pcntl_alarm') && function_exists('pcntl_signal');
        $timeout = $job->getTimeout();

        if ($hasAlarm && $timeout > 0) {
            pcntl_signal(SIGALRM, function () use ($timeout) {
                throw new JobTimeoutException("Job exceeded execution timeout of {$timeout} seconds.");
            });
            pcntl_alarm($timeout);
        }

        try {
            if (method_exists($instance, 'handle')) {
                return $instance->handle($job->getData());
            }

            if (is_callable($instance)) {
                return $instance($job->getData());
            }

            throw new \RuntimeException("Job class '{$jobClass}' does not implement handle() or __invoke().");
        } finally {
            if ($hasAlarm && $timeout > 0) {
                pcntl_alarm(0); // Cancel alarm
            }
        }
    }

    public function getProcessedCount(): int
    {
        return $this->processedCount;
    }

    public function getFailedCount(): int
    {
        return $this->failedCount;
    }
}
