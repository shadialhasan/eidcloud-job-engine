<?php

declare(strict_types=1);

namespace EidCloud\JobEngine\Drivers;

use EidCloud\JobEngine\Contracts\QueueDriverInterface;
use EidCloud\JobEngine\Exceptions\QueueDriverException;
use EidCloud\JobEngine\Models\JobPayload;

class FileQueueDriver implements QueueDriverInterface
{
    private string $storageDir;
    private string $lockFile;

    public function __construct(string $storageDir)
    {
        $this->storageDir = rtrim($storageDir, '/\\');
        if (!is_dir($this->storageDir)) {
            mkdir($this->storageDir, 0777, true);
        }
        $this->lockFile = $this->storageDir . DIRECTORY_SEPARATOR . '.queue.lock';
        $this->initializeDirectories();
    }

    private function initializeDirectories(): void
    {
        $dirs = ['pending', 'processing', 'completed', 'dead_letters'];
        foreach ($dirs as $dir) {
            $path = $this->storageDir . DIRECTORY_SEPARATOR . $dir;
            if (!is_dir($path)) {
                mkdir($path, 0777, true);
            }
        }
    }

    /**
     * Run an atomic operation synchronized via file lock.
     *
     * @template T
     * @param callable(): T $callback
     * @return T
     */
    private function withLock(callable $callback): mixed
    {
        $fp = fopen($this->lockFile, 'c+');
        if (!$fp) {
            throw new QueueDriverException("Unable to acquire lock file: {$this->lockFile}");
        }

        try {
            if (!flock($fp, LOCK_EX)) {
                throw new QueueDriverException("Failed to lock file: {$this->lockFile}");
            }
            return $callback();
        } finally {
            flock($fp, LOCK_UN);
            fclose($fp);
        }
    }

    public function push(JobPayload $job): string
    {
        return $this->withLock(function () use ($job) {
            $fileName = $this->generateFileName($job);
            $targetPath = $this->storageDir . DIRECTORY_SEPARATOR . 'pending' . DIRECTORY_SEPARATOR . $fileName;
            file_put_contents($targetPath, $job->toJson(), LOCK_EX);
            return $job->getId();
        });
    }

    private function generateFileName(JobPayload $job): string
    {
        // Format: {availableAt}_{priorityPad}_{queue}_{jobId}.json
        // Inverted priority padding so higher priority sorts first alphabetically:
        // priority 100 -> (9999 - 100) = 09899
        $invPrio = sprintf('%05d', 9999 - min(9999, max(0, $job->getPriority())));
        $avail = sprintf('%011d', $job->getAvailableAt());
        $cleanQueue = preg_replace('/[^a-zA-Z0-9_\-]/', '_', $job->getQueue());
        return "{$avail}_{$invPrio}_{$cleanQueue}_{$job->getId()}.json";
    }

    public function pop(array|string $queues): ?JobPayload
    {
        return $this->withLock(function () use ($queues) {
            $queueList = (array)$queues;
            $pendingDir = $this->storageDir . DIRECTORY_SEPARATOR . 'pending';
            $files = scandir($pendingDir);
            if ($files === false) {
                return null;
            }

            $now = time();
            $candidates = [];

            foreach ($files as $file) {
                if ($file === '.' || $file === '..' || !str_ends_with($file, '.json')) {
                    continue;
                }

                $parts = explode('_', $file, 4);
                if (count($parts) < 4) {
                    continue;
                }

                $availableAt = (int)$parts[0];
                $invPrio = (int)$parts[1];
                $queueName = $parts[2];

                if ($availableAt > $now) {
                    continue; // Not yet available
                }

                if (!empty($queueList) && !in_array($queueName, $queueList, true)) {
                    continue; // Queue does not match
                }

                $candidates[] = [
                    'file' => $file,
                    'availableAt' => $availableAt,
                    'invPrio' => $invPrio,
                ];
            }

            if (empty($candidates)) {
                return null;
            }

            // Sort candidates: lowest invPrio (highest actual priority) first, then lowest availableAt
            usort($candidates, function ($a, $b) {
                if ($a['invPrio'] !== $b['invPrio']) {
                    return $a['invPrio'] <=> $b['invPrio'];
                }
                return $a['availableAt'] <=> $b['availableAt'];
            });

            $bestFile = $candidates[0]['file'];
            $srcPath = $pendingDir . DIRECTORY_SEPARATOR . $bestFile;
            $content = file_get_contents($srcPath);
            if ($content === false) {
                return null;
            }

            $job = JobPayload::fromJson($content);
            $job->setStartedAt($now);
            $job->incrementAttempts();

            $procDir = $this->storageDir . DIRECTORY_SEPARATOR . 'processing';
            $destPath = $procDir . DIRECTORY_SEPARATOR . $job->getId() . '.json';

            file_put_contents($destPath, $job->toJson(), LOCK_EX);
            @unlink($srcPath);

            return $job;
        });
    }

    public function acknowledge(string $jobId): bool
    {
        return $this->withLock(function () use ($jobId) {
            $procPath = $this->storageDir . DIRECTORY_SEPARATOR . 'processing' . DIRECTORY_SEPARATOR . $jobId . '.json';
            if (!file_exists($procPath)) {
                return false;
            }

            $content = file_get_contents($procPath);
            if ($content !== false) {
                $job = JobPayload::fromJson($content);
                $job->setCompletedAt(time());
                $compPath = $this->storageDir . DIRECTORY_SEPARATOR . 'completed' . DIRECTORY_SEPARATOR . $jobId . '.json';
                file_put_contents($compPath, $job->toJson(), LOCK_EX);
            }

            @unlink($procPath);
            return true;
        });
    }

    public function release(JobPayload $job, int $delaySeconds = 0): bool
    {
        return $this->withLock(function () use ($job, $delaySeconds) {
            $procPath = $this->storageDir . DIRECTORY_SEPARATOR . 'processing' . DIRECTORY_SEPARATOR . $job->getId() . '.json';
            @unlink($procPath);

            $availableAt = time() + max(0, $delaySeconds);
            $job->setAvailableAt($availableAt);

            $fileName = $this->generateFileName($job);
            $targetPath = $this->storageDir . DIRECTORY_SEPARATOR . 'pending' . DIRECTORY_SEPARATOR . $fileName;
            file_put_contents($targetPath, $job->toJson(), LOCK_EX);
            return true;
        });
    }

    public function moveToDeadLetter(JobPayload $job, string $reason = ''): bool
    {
        return $this->withLock(function () use ($job, $reason) {
            $procPath = $this->storageDir . DIRECTORY_SEPARATOR . 'processing' . DIRECTORY_SEPARATOR . $job->getId() . '.json';
            @unlink($procPath);

            if ($reason) {
                $job->setLastError($reason);
            }

            $dlqDir = $this->storageDir . DIRECTORY_SEPARATOR . 'dead_letters';
            $dlqRecord = [
                'id' => $job->getId(),
                'queue' => $job->getQueue(),
                'job_class' => $job->getJobClass(),
                'payload' => $job->toJson(),
                'failed_at' => time(),
                'attempts' => $job->getAttempts(),
                'error_message' => $job->getLastError() ?? $reason,
            ];

            $dlqPath = $dlqDir . DIRECTORY_SEPARATOR . $job->getId() . '.json';
            file_put_contents($dlqPath, json_encode($dlqRecord, JSON_PRETTY_PRINT), LOCK_EX);
            return true;
        });
    }

    public function getDeadLetters(int $limit = 50, int $offset = 0): array
    {
        return $this->withLock(function () use ($limit, $offset) {
            $dlqDir = $this->storageDir . DIRECTORY_SEPARATOR . 'dead_letters';
            $files = scandir($dlqDir);
            if ($files === false) {
                return [];
            }

            $items = [];
            foreach ($files as $file) {
                if ($file === '.' || $file === '..' || !str_ends_with($file, '.json')) {
                    continue;
                }
                $content = file_get_contents($dlqDir . DIRECTORY_SEPARATOR . $file);
                if ($content !== false) {
                    $decoded = json_decode($content, true);
                    if (is_array($decoded)) {
                        $items[] = $decoded;
                    }
                }
            }

            usort($items, fn($a, $b) => ($b['failed_at'] ?? 0) <=> ($a['failed_at'] ?? 0));
            return array_slice($items, $offset, $limit);
        });
    }

    public function replayDeadLetter(string $jobId): bool
    {
        return $this->withLock(function () use ($jobId) {
            $dlqPath = $this->storageDir . DIRECTORY_SEPARATOR . 'dead_letters' . DIRECTORY_SEPARATOR . $jobId . '.json';
            if (!file_exists($dlqPath)) {
                return false;
            }

            $content = file_get_contents($dlqPath);
            if ($content === false) {
                return false;
            }

            $record = json_decode($content, true);
            if (!isset($record['payload'])) {
                return false;
            }

            $job = JobPayload::fromJson($record['payload']);
            $job->setAvailableAt(time());
            $job->setLastError(null);

            @unlink($dlqPath);

            $fileName = $this->generateFileName($job);
            $targetPath = $this->storageDir . DIRECTORY_SEPARATOR . 'pending' . DIRECTORY_SEPARATOR . $fileName;
            file_put_contents($targetPath, $job->toJson(), LOCK_EX);
            return true;
        });
    }

    public function purgeDeadLetter(?string $jobId = null): int
    {
        return $this->withLock(function () use ($jobId) {
            $dlqDir = $this->storageDir . DIRECTORY_SEPARATOR . 'dead_letters';
            if ($jobId !== null) {
                $path = $dlqDir . DIRECTORY_SEPARATOR . $jobId . '.json';
                if (file_exists($path)) {
                    @unlink($path);
                    return 1;
                }
                return 0;
            }

            $files = scandir($dlqDir);
            $count = 0;
            if ($files !== false) {
                foreach ($files as $file) {
                    if ($file !== '.' && $file !== '..' && str_ends_with($file, '.json')) {
                        @unlink($dlqDir . DIRECTORY_SEPARATOR . $file);
                        $count++;
                    }
                }
            }
            return $count;
        });
    }

    public function size(array|string|null $queues = null): int
    {
        return $this->withLock(function () use ($queues) {
            $pendingDir = $this->storageDir . DIRECTORY_SEPARATOR . 'pending';
            $files = scandir($pendingDir);
            if ($files === false) {
                return 0;
            }

            $queueList = $queues !== null ? (array)$queues : null;
            $count = 0;
            $now = time();

            foreach ($files as $file) {
                if ($file === '.' || $file === '..' || !str_ends_with($file, '.json')) {
                    continue;
                }
                $parts = explode('_', $file, 4);
                if (count($parts) < 4) {
                    continue;
                }
                $avail = (int)$parts[0];
                $queueName = $parts[2];

                if ($avail <= $now) {
                    if ($queueList === null || in_array($queueName, $queueList, true)) {
                        $count++;
                    }
                }
            }
            return $count;
        });
    }

    public function stats(): array
    {
        return $this->withLock(function () {
            $now = time();
            $pending = 0;
            $delayed = 0;
            $processing = 0;
            $completed = 0;
            $deadLetters = 0;

            $pendingFiles = @scandir($this->storageDir . DIRECTORY_SEPARATOR . 'pending') ?: [];
            foreach ($pendingFiles as $f) {
                if (str_ends_with($f, '.json')) {
                    $parts = explode('_', $f, 4);
                    if (isset($parts[0]) && (int)$parts[0] > $now) {
                        $delayed++;
                    } else {
                        $pending++;
                    }
                }
            }

            $procFiles = @scandir($this->storageDir . DIRECTORY_SEPARATOR . 'processing') ?: [];
            foreach ($procFiles as $f) {
                if (str_ends_with($f, '.json')) {
                    $processing++;
                }
            }

            $compFiles = @scandir($this->storageDir . DIRECTORY_SEPARATOR . 'completed') ?: [];
            foreach ($compFiles as $f) {
                if (str_ends_with($f, '.json')) {
                    $completed++;
                }
            }

            $dlqFiles = @scandir($this->storageDir . DIRECTORY_SEPARATOR . 'dead_letters') ?: [];
            foreach ($dlqFiles as $f) {
                if (str_ends_with($f, '.json')) {
                    $deadLetters++;
                }
            }

            return [
                'pending' => $pending,
                'processing' => $processing,
                'delayed' => $delayed,
                'completed' => $completed,
                'dead_letters' => $deadLetters,
                'total' => $pending + $delayed + $processing + $completed,
            ];
        });
    }

    public function clear(?string $queue = null): void
    {
        $this->withLock(function () use ($queue) {
            $dirs = ['pending', 'processing', 'completed', 'dead_letters'];
            foreach ($dirs as $d) {
                $dirPath = $this->storageDir . DIRECTORY_SEPARATOR . $d;
                $files = @scandir($dirPath) ?: [];
                foreach ($files as $file) {
                    if ($file !== '.' && $file !== '..' && str_ends_with($file, '.json')) {
                        if ($queue !== null) {
                            $content = @file_get_contents($dirPath . DIRECTORY_SEPARATOR . $file);
                            if ($content) {
                                $data = @json_decode($content, true);
                                if (($data['queue'] ?? null) === $queue) {
                                    @unlink($dirPath . DIRECTORY_SEPARATOR . $file);
                                }
                            }
                        } else {
                            @unlink($dirPath . DIRECTORY_SEPARATOR . $file);
                        }
                    }
                }
            }
        });
    }
}
