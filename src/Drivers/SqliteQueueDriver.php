<?php

declare(strict_types=1);

namespace EidCloud\JobEngine\Drivers;

use EidCloud\JobEngine\Contracts\QueueDriverInterface;
use EidCloud\JobEngine\Exceptions\QueueDriverException;
use EidCloud\JobEngine\Models\JobPayload;
use PDO;
use PDOException;

class SqliteQueueDriver implements QueueDriverInterface
{
    private PDO $pdo;
    private string $path;

    public function __construct(string $path = ':memory:')
    {
        $this->path = $path;
        $this->connect();
        $this->initializeSchema();
    }

    private function connect(): void
    {
        try {
            if ($this->path !== ':memory:') {
                $dir = dirname($this->path);
                if (!is_dir($dir)) {
                    mkdir($dir, 0777, true);
                }
            }

            $this->pdo = new PDO("sqlite:{$this->path}");
            $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
            $this->pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);

            // Busy timeout for concurrency
            $this->pdo->exec('PRAGMA busy_timeout = 5000;');
            if ($this->path !== ':memory:') {
                $this->pdo->exec('PRAGMA journal_mode = WAL;');
                $this->pdo->exec('PRAGMA synchronous = NORMAL;');
            }
        } catch (PDOException $e) {
            throw new QueueDriverException("Failed to connect to SQLite queue database: " . $e->getMessage(), 0, $e);
        }
    }

    private function initializeSchema(): void
    {
        $sql = "
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                queue TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 50,
                status TEXT NOT NULL DEFAULT 'pending',
                job_class TEXT NOT NULL,
                payload TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 3,
                available_at INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                started_at INTEGER DEFAULT NULL,
                completed_at INTEGER DEFAULT NULL,
                last_error TEXT DEFAULT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_jobs_queue_status_avail_prio 
                ON jobs(queue, status, available_at, priority DESC);

            CREATE TABLE IF NOT EXISTS dead_letters (
                id TEXT PRIMARY KEY,
                queue TEXT NOT NULL,
                job_class TEXT NOT NULL,
                payload TEXT NOT NULL,
                failed_at INTEGER NOT NULL,
                attempts INTEGER NOT NULL,
                error_message TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_dlq_failed_at ON dead_letters(failed_at DESC);
        ";

        $this->pdo->exec($sql);
    }

    public function push(JobPayload $job): string
    {
        $stmt = $this->pdo->prepare("
            INSERT OR REPLACE INTO jobs (
                id, queue, priority, status, job_class, payload, 
                attempts, max_attempts, available_at, created_at, started_at, completed_at, last_error
            ) VALUES (
                :id, :queue, :priority, :status, :job_class, :payload,
                :attempts, :max_attempts, :available_at, :created_at, :started_at, :completed_at, :last_error
            )
        ");

        $stmt->execute([
            ':id' => $job->getId(),
            ':queue' => $job->getQueue(),
            ':priority' => $job->getPriority(),
            ':status' => JobPayload::STATUS_PENDING,
            ':job_class' => $job->getJobClass(),
            ':payload' => $job->toJson(),
            ':attempts' => $job->getAttempts(),
            ':max_attempts' => $job->getMaxAttempts(),
            ':available_at' => $job->getAvailableAt(),
            ':created_at' => $job->getCreatedAt(),
            ':started_at' => $job->getStartedAt(),
            ':completed_at' => $job->getCompletedAt(),
            ':last_error' => $job->getLastError(),
        ]);

        return $job->getId();
    }

    public function pop(array|string $queues): ?JobPayload
    {
        $queueList = (array)$queues;
        if (empty($queueList)) {
            $queueList = ['default'];
        }

        $placeholders = implode(',', array_fill(0, count($queueList), '?'));
        $now = time();

        $this->pdo->beginTransaction();
        try {
            // Find highest priority available pending job matching queue list
            $sql = "
                SELECT id, payload, attempts FROM jobs
                WHERE queue IN ($placeholders)
                  AND status = 'pending'
                  AND available_at <= ?
                ORDER BY priority DESC, available_at ASC, id ASC
                LIMIT 1
            ";

            $params = array_merge($queueList, [$now]);
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute($params);
            $row = $stmt->fetch();

            if (!$row) {
                $this->pdo->commit();
                return null;
            }

            $jobId = $row['id'];
            $attempts = (int)$row['attempts'] + 1;

            // Mark job as processing
            $updateStmt = $this->pdo->prepare("
                UPDATE jobs 
                SET status = 'processing', attempts = ?, started_at = ?
                WHERE id = ?
            ");
            $updateStmt->execute([$attempts, $now, $jobId]);

            $this->pdo->commit();

            $job = JobPayload::fromJson($row['payload']);
            $job->setStartedAt($now);
            // Reflect incremented attempts
            while ($job->getAttempts() < $attempts) {
                $job->incrementAttempts();
            }

            return $job;
        } catch (\Throwable $e) {
            if ($this->pdo->inTransaction()) {
                $this->pdo->rollBack();
            }
            throw new QueueDriverException("Failed to pop job from SQLite queue: " . $e->getMessage(), 0, $e);
        }
    }

    public function acknowledge(string $jobId): bool
    {
        $stmt = $this->pdo->prepare("
            UPDATE jobs 
            SET status = 'completed', completed_at = ? 
            WHERE id = ?
        ");
        return $stmt->execute([time(), $jobId]);
    }

    public function release(JobPayload $job, int $delaySeconds = 0): bool
    {
        $availableAt = time() + max(0, $delaySeconds);
        $job->setAvailableAt($availableAt);

        $stmt = $this->pdo->prepare("
            UPDATE jobs 
            SET status = 'pending', available_at = ?, attempts = ?, last_error = ?, payload = ?
            WHERE id = ?
        ");

        return $stmt->execute([
            $availableAt,
            $job->getAttempts(),
            $job->getLastError(),
            $job->toJson(),
            $job->getId(),
        ]);
    }

    public function moveToDeadLetter(JobPayload $job, string $reason = ''): bool
    {
        $this->pdo->beginTransaction();
        try {
            $updateStmt = $this->pdo->prepare("
                UPDATE jobs 
                SET status = 'dead', last_error = ? 
                WHERE id = ?
            ");
            $updateStmt->execute([$reason, $job->getId()]);

            $insertStmt = $this->pdo->prepare("
                INSERT OR REPLACE INTO dead_letters (
                    id, queue, job_class, payload, failed_at, attempts, error_message
                ) VALUES (
                    :id, :queue, :job_class, :payload, :failed_at, :attempts, :error_message
                )
            ");
            $insertStmt->execute([
                ':id' => $job->getId(),
                ':queue' => $job->getQueue(),
                ':job_class' => $job->getJobClass(),
                ':payload' => $job->toJson(),
                ':failed_at' => time(),
                ':attempts' => $job->getAttempts(),
                ':error_message' => $reason ?: $job->getLastError(),
            ]);

            $this->pdo->commit();
            return true;
        } catch (\Throwable $e) {
            if ($this->pdo->inTransaction()) {
                $this->pdo->rollBack();
            }
            throw new QueueDriverException("Failed to move job to DLQ: " . $e->getMessage(), 0, $e);
        }
    }

    public function getDeadLetters(int $limit = 50, int $offset = 0): array
    {
        $stmt = $this->pdo->prepare("
            SELECT id, queue, job_class, payload, failed_at, attempts, error_message
            FROM dead_letters
            ORDER BY failed_at DESC
            LIMIT ? OFFSET ?
        ");
        $stmt->bindValue(1, $limit, PDO::PARAM_INT);
        $stmt->bindValue(2, $offset, PDO::PARAM_INT);
        $stmt->execute();
        return $stmt->fetchAll();
    }

    public function replayDeadLetter(string $jobId): bool
    {
        $this->pdo->beginTransaction();
        try {
            $stmt = $this->pdo->prepare("SELECT payload FROM dead_letters WHERE id = ?");
            $stmt->execute([$jobId]);
            $row = $stmt->fetch();

            if (!$row) {
                $this->pdo->rollBack();
                return false;
            }

            $job = JobPayload::fromJson($row['payload']);
            $job->setAvailableAt(time());

            // Delete from dead letters
            $delStmt = $this->pdo->prepare("DELETE FROM dead_letters WHERE id = ?");
            $delStmt->execute([$jobId]);

            // Re-push / update job to pending
            $updateStmt = $this->pdo->prepare("
                UPDATE jobs 
                SET status = 'pending', available_at = ?, attempts = 0, last_error = NULL, payload = ?
                WHERE id = ?
            ");
            $updateStmt->execute([time(), $job->toJson(), $jobId]);

            $this->pdo->commit();
            return true;
        } catch (\Throwable $e) {
            if ($this->pdo->inTransaction()) {
                $this->pdo->rollBack();
            }
            throw new QueueDriverException("Failed to replay dead letter: " . $e->getMessage(), 0, $e);
        }
    }

    public function purgeDeadLetter(?string $jobId = null): int
    {
        if ($jobId !== null) {
            $stmt = $this->pdo->prepare("DELETE FROM dead_letters WHERE id = ?");
            $stmt->execute([$jobId]);
            return $stmt->rowCount();
        }

        $stmt = $this->pdo->prepare("DELETE FROM dead_letters");
        $stmt->execute();
        return $stmt->rowCount();
    }

    public function size(array|string|null $queues = null): int
    {
        if ($queues === null) {
            $stmt = $this->pdo->query("SELECT COUNT(*) as cnt FROM jobs WHERE status = 'pending'");
            return (int)$stmt->fetchColumn();
        }

        $queueList = (array)$queues;
        if (empty($queueList)) {
            return 0;
        }

        $placeholders = implode(',', array_fill(0, count($queueList), '?'));
        $stmt = $this->pdo->prepare("SELECT COUNT(*) as cnt FROM jobs WHERE status = 'pending' AND queue IN ($placeholders)");
        $stmt->execute($queueList);
        return (int)$stmt->fetchColumn();
    }

    public function stats(): array
    {
        $now = time();
        $stats = [
            'pending' => 0,
            'processing' => 0,
            'delayed' => 0,
            'completed' => 0,
            'dead_letters' => 0,
            'total' => 0,
        ];

        $stmt = $this->pdo->query("
            SELECT 
                status,
                available_at,
                COUNT(*) as count
            FROM jobs
            GROUP BY status, (available_at > $now)
        ");

        while ($row = $stmt->fetch()) {
            $status = $row['status'];
            $count = (int)$row['count'];
            $stats['total'] += $count;

            if ($status === 'pending') {
                if ((int)$row['available_at'] > $now) {
                    $stats['delayed'] += $count;
                } else {
                    $stats['pending'] += $count;
                }
            } elseif ($status === 'processing') {
                $stats['processing'] += $count;
            } elseif ($status === 'completed') {
                $stats['completed'] += $count;
            }
        }

        $dlqStmt = $this->pdo->query("SELECT COUNT(*) as cnt FROM dead_letters");
        $stats['dead_letters'] = (int)$dlqStmt->fetchColumn();

        return $stats;
    }

    public function clear(?string $queue = null): void
    {
        if ($queue !== null) {
            $stmt = $this->pdo->prepare("DELETE FROM jobs WHERE queue = ?");
            $stmt->execute([$queue]);
            $stmtDlq = $this->pdo->prepare("DELETE FROM dead_letters WHERE queue = ?");
            $stmtDlq->execute([$queue]);
        } else {
            $this->pdo->exec("DELETE FROM jobs");
            $this->pdo->exec("DELETE FROM dead_letters");
        }
    }
}
