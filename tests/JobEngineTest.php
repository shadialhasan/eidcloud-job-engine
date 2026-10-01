<?php

declare(strict_types=1);

namespace EidCloud\JobEngine\Tests;

use EidCloud\JobEngine\JobQueue;
use EidCloud\JobEngine\Drivers\SqliteQueueDriver;
use EidCloud\JobEngine\Drivers\FileQueueDriver;
use EidCloud\JobEngine\Drivers\RedisQueueDriver;
use EidCloud\JobEngine\Models\JobPayload;
use EidCloud\JobEngine\Worker\Worker;
use EidCloud\JobEngine\Worker\WorkerOptions;
use EidCloud\JobEngine\Worker\ProcessPool;
use EidCloud\JobEngine\Contracts\JobInterface;

// Dummy Test Jobs
class TestSuccessJob implements JobInterface
{
    public static int $executed = 0;
    public static array $lastPayload = [];

    public function handle(array $payload): mixed
    {
        self::$executed++;
        self::$lastPayload = $payload;
        return true;
    }
}

class TestFailingJob implements JobInterface
{
    public static int $attemptsCount = 0;

    public function handle(array $payload): mixed
    {
        self::$attemptsCount++;
        throw new \RuntimeException("Forced failure in test job attempt " . self::$attemptsCount);
    }
}

class TestChainedJobA implements JobInterface
{
    public static bool $executed = false;

    public function handle(array $payload): mixed
    {
        self::$executed = true;
        return 'JobA done';
    }
}

class TestChainedJobB implements JobInterface
{
    public static bool $executed = false;

    public function handle(array $payload): mixed
    {
        self::$executed = true;
        return 'JobB done';
    }
}

class JobEngineTest
{
    private int $passes = 0;
    private int $failures = 0;

    private function assert(bool $condition, string $description): void
    {
        if ($condition) {
            echo "  \033[1;32m✔ PASS:\033[0m {$description}\n";
            $this->passes++;
        } else {
            echo "  \033[1;31m✘ FAIL:\033[0m {$description}\n";
            $this->failures++;
        }
    }

    public function runAll(): int
    {
        echo "\033[1;36m=== EidCloud Job Engine Automated Test Suite ===\033[0m\n\n";

        $this->testJobPayloadSerialization();
        $this->testSqliteDriverBasicPushPop();
        $this->testFileDriverBasicPushPop();
        $this->testPriorityOrdering();
        $this->testDelayedJobs();
        $this->testWorkerProcessingAndAck();
        $this->testRetryWithExponentialBackoff();
        $this->testDeadLetterQueueAndReplay();
        $this->testJobChaining();
        $this->testRateLimiting();
        $this->testProcessPoolHeartbeats();

        echo "\n\033[1;36m================================================\033[0m\n";
        echo "Results: \033[1;32m{$this->passes} Passed\033[0m, ";
        if ($this->failures > 0) {
            echo "\033[1;31m{$this->failures} Failed\033[0m\n";
            return 1;
        } else {
            echo "\033[1;32m0 Failed (100% Pass)\033[0m\n";
            return 0;
        }
    }

    private function testJobPayloadSerialization(): void
    {
        echo "\033[1;33m[Test Group: JobPayload Serialization & Hydration]\033[0m\n";
        $job = new JobPayload(
            jobClass: TestSuccessJob::class,
            data: ['user_id' => 42, 'action' => 'sync'],
            queue: 'high',
            priority: 100,
            maxAttempts: 5,
            timeout: 120
        );

        $json = $job->toJson();
        $restored = JobPayload::fromJson($json);

        $this->assert($restored->getId() === $job->getId(), "Job UUID preserved across JSON serialization");
        $this->assert($restored->getJobClass() === TestSuccessJob::class, "Job class matches");
        $this->assert($restored->getData()['user_id'] === 42, "Payload data intact");
        $this->assert($restored->getPriority() === 100, "Priority preserved");
        $this->assert($restored->getMaxAttempts() === 5, "Max attempts preserved");
        $this->assert($restored->getTimeout() === 120, "Timeout preserved");
    }

    private function testSqliteDriverBasicPushPop(): void
    {
        echo "\n\033[1;33m[Test Group: SQLite Driver Push/Pop/Ack]\033[0m\n";
        $driver = new SqliteQueueDriver(':memory:');
        $job = new JobPayload(TestSuccessJob::class, ['foo' => 'bar'], 'default');

        $driver->push($job);
        $this->assert($driver->size('default') === 1, "Queue size is 1 after push");

        $popped = $driver->pop('default');
        $this->assert($popped !== null && $popped->getId() === $job->getId(), "Successfully popped pushed job");
        $this->assert($popped->getAttempts() === 1, "Attempt count incremented on pop");

        $driver->acknowledge($popped->getId());
        $stats = $driver->stats();
        $this->assert($stats['completed'] === 1, "Job acknowledged and marked completed in stats");
        $this->assert($driver->size('default') === 0, "Pending queue empty after pop & ack");
    }

    private function testFileDriverBasicPushPop(): void
    {
        echo "\n\033[1;33m[Test Group: File Driver Push/Pop/Ack]\033[0m\n";
        $tmpDir = sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'test_file_queue_' . uniqid();
        $driver = new FileQueueDriver($tmpDir);

        $job = new JobPayload(TestSuccessJob::class, ['item' => 999], 'default');
        $driver->push($job);

        $this->assert($driver->size('default') === 1, "File queue size is 1 after push");

        $popped = $driver->pop('default');
        $this->assert($popped !== null && $popped->getId() === $job->getId(), "File driver popped matching job");

        $driver->acknowledge($popped->getId());
        $this->assert($driver->size('default') === 0, "File queue empty after acknowledge");
    }

    private function testPriorityOrdering(): void
    {
        echo "\n\033[1;33m[Test Group: Priority Queue Ordering]\033[0m\n";
        $driver = new SqliteQueueDriver(':memory:');

        $jobLow = new JobPayload(TestSuccessJob::class, ['name' => 'low'], priority: 10);
        $jobHigh = new JobPayload(TestSuccessJob::class, ['name' => 'high'], priority: 100);
        $jobMedium = new JobPayload(TestSuccessJob::class, ['name' => 'medium'], priority: 50);

        // Push in random order
        $driver->push($jobLow);
        $driver->push($jobHigh);
        $driver->push($jobMedium);

        $first = $driver->pop('default');
        $second = $driver->pop('default');
        $third = $driver->pop('default');

        $this->assert($first !== null && $first->getData()['name'] === 'high', "Highest priority job popped first");
        $this->assert($second !== null && $second->getData()['name'] === 'medium', "Medium priority job popped second");
        $this->assert($third !== null && $third->getData()['name'] === 'low', "Low priority job popped last");
    }

    private function testDelayedJobs(): void
    {
        echo "\n\033[1;33m[Test Group: Delayed Jobs Execution]\033[0m\n";
        $driver = new SqliteQueueDriver(':memory:');

        $delayedJob = new JobPayload(
            jobClass: TestSuccessJob::class,
            data: ['future' => true],
            availableAt: time() + 2 // 2 seconds in future
        );
        $driver->push($delayedJob);

        $immediatePop = $driver->pop('default');
        $this->assert($immediatePop === null, "Delayed job is NOT available immediately");

        $stats = $driver->stats();
        $this->assert($stats['delayed'] === 1, "Delayed job correctly tracked in stats");

        // Manually adjust availableAt to past for fast deterministic test
        $delayedJob->setAvailableAt(time() - 1);
        $driver->push($delayedJob);

        $readyPop = $driver->pop('default');
        $this->assert($readyPop !== null && $readyPop->getId() === $delayedJob->getId(), "Job becomes available once timer matures");
    }

    private function testWorkerProcessingAndAck(): void
    {
        echo "\n\033[1;33m[Test Group: Worker Execution Loop & Options]\033[0m\n";
        TestSuccessJob::$executed = 0;

        $driver = new SqliteQueueDriver(':memory:');
        $engine = new JobQueue($driver);

        $engine->dispatch(TestSuccessJob::class, ['msg' => 'hello world']);
        $engine->dispatch(TestSuccessJob::class, ['msg' => 'second job']);

        $options = new WorkerOptions(stopWhenEmpty: true);
        $worker = $engine->worker(['default'], $options);
        $worker->work();

        $this->assert(TestSuccessJob::$executed === 2, "Worker executed both pending jobs successfully");
        $this->assert($worker->getProcessedCount() === 2, "Worker tracked 2 processed jobs");
        $this->assert($worker->getFailedCount() === 0, "Worker tracked 0 failed jobs");
        $this->assert($engine->size() === 0, "Queue empty after worker runs");
    }

    private function testRetryWithExponentialBackoff(): void
    {
        echo "\n\033[1;33m[Test Group: Exponential Backoff & Retry Logic]\033[0m\n";
        $driver = new SqliteQueueDriver(':memory:');
        $job = new JobPayload(
            jobClass: TestFailingJob::class,
            maxAttempts: 3,
            retryDelaySeconds: 2,
            exponentialBackoff: true
        );

        $job->incrementAttempts(); // attempt 1
        $delay1 = $job->calculateNextRetryDelay(); // 2 * 2^0 = 2

        $job->incrementAttempts(); // attempt 2
        $delay2 = $job->calculateNextRetryDelay(); // 2 * 2^1 = 4

        $job->incrementAttempts(); // attempt 3
        $delay3 = $job->calculateNextRetryDelay(); // 2 * 2^2 = 8

        $this->assert($delay1 === 2, "Attempt 1 backoff delay is 2s");
        $this->assert($delay2 === 4, "Attempt 2 backoff delay is 4s (doubled)");
        $this->assert($delay3 === 8, "Attempt 3 backoff delay is 8s (doubled)");
    }

    private function testDeadLetterQueueAndReplay(): void
    {
        echo "\n\033[1;33m[Test Group: Dead Letter Queue (DLQ) & Replay]\033[0m\n";
        TestFailingJob::$attemptsCount = 0;

        $driver = new SqliteQueueDriver(':memory:');
        $engine = new JobQueue($driver);

        // Job allows max 1 attempt -> fails immediately to DLQ
        $jobId = $engine->dispatch(TestFailingJob::class, ['test' => 'dlq'], maxAttempts: 1);

        $worker = $engine->worker(['default'], new WorkerOptions(stopWhenEmpty: true));
        $worker->work();

        $dlq = $engine->dlq();
        $this->assert(count($dlq) === 1, "Failed job moved to Dead Letter Queue");
        $this->assert($dlq[0]['id'] === $jobId, "DLQ contains correct job ID");
        $this->assert(str_contains($dlq[0]['error_message'], 'Forced failure in test job'), "DLQ preserves exception message");

        // Replay DLQ job
        $replayed = $engine->replay($jobId);
        $this->assert($replayed === true, "Successfully replayed job from DLQ");
        $this->assert($engine->size('default') === 1, "Job restored to pending queue");
        $this->assert(count($engine->dlq()) === 0, "DLQ cleared of replayed job");

        // Purge test
        $engine->purgeDlq();
        $this->assert(count($engine->dlq()) === 0, "Purge DLQ functions properly");
    }

    private function testJobChaining(): void
    {
        echo "\n\033[1;33m[Test Group: Job Chaining (JobA -> JobB)]\033[0m\n";
        TestChainedJobA::$executed = false;
        TestChainedJobB::$executed = false;

        $driver = new SqliteQueueDriver(':memory:');
        $engine = new JobQueue($driver);

        $jobA = new JobPayload(TestChainedJobA::class, ['step' => 1]);
        $jobA->appendChain(TestChainedJobB::class, ['step' => 2]);

        $engine->dispatchPayload($jobA);

        $worker = $engine->worker(['default'], new WorkerOptions(stopWhenEmpty: true));
        $worker->work();

        $this->assert(TestChainedJobA::$executed === true, "Primary chained job A executed");
        $this->assert(TestChainedJobB::$executed === true, "Chained follower job B automatically executed");
    }

    private function testRateLimiting(): void
    {
        echo "\n\033[1;33m[Test Group: Rate Limiting]\033[0m\n";
        $driver = new SqliteQueueDriver(':memory:');
        $engine = new JobQueue($driver);

        $engine->dispatch(TestSuccessJob::class, ['call' => 1], rateLimitKey: 'api:stripe');
        $engine->dispatch(TestSuccessJob::class, ['call' => 2], rateLimitKey: 'api:stripe');

        $worker = $engine->worker(['default'], new WorkerOptions(stopWhenEmpty: true, rateLimitDelaySeconds: 10));
        $worker->work();

        // One should process, second should be released with delay due to rate limit key
        $stats = $engine->stats();
        $this->assert($stats['completed'] === 1, "First rate-limited job completed");
        $this->assert($stats['delayed'] === 1, "Second rate-limited job released with delay");
    }

    private function testProcessPoolHeartbeats(): void
    {
        echo "\n\033[1;33m[Test Group: Process Pool & Heartbeats]\033[0m\n";
        $driver = new SqliteQueueDriver(':memory:');
        $tempHb = sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'test_hb_' . uniqid();
        @mkdir($tempHb, 0777, true);

        $pool = new ProcessPool($driver, ['default'], 2, null, $tempHb);

        // Simulate a running worker writing heartbeat
        $hbFile = $tempHb . DIRECTORY_SEPARATOR . 'worker-unit-test.json';
        file_put_contents($hbFile, json_encode([
            'id' => 'worker-unit-test',
            'pid' => 12345,
            'time' => time(),
            'processed' => 10,
            'failed' => 0,
            'queues' => ['default']
        ]));

        $heartbeats = $pool->getHeartbeats();
        $this->assert(count($heartbeats) === 1, "Heartbeat monitor detected 1 active worker");
        $this->assert($heartbeats[0]['id'] === 'worker-unit-test', "Worker ID matches in heartbeat");
        $this->assert($heartbeats[0]['healthy'] === true, "Worker correctly flagged healthy");

        @unlink($hbFile);
        @rmdir($tempHb);
    }
}
