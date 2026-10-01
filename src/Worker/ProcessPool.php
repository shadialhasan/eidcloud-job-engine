<?php

declare(strict_types=1);

namespace EidCloud\JobEngine\Worker;

use EidCloud\JobEngine\Contracts\QueueDriverInterface;
use Throwable;

class ProcessPool
{
    private QueueDriverInterface $driver;
    private array $queues;
    private int $numWorkers;
    private WorkerOptions $options;
    private string $heartbeatDir;
    private array $workers = []; // pid/workerId => resource or worker
    private bool $shouldStop = false;

    public function __construct(
        QueueDriverInterface $driver,
        array|string $queues = ['default'],
        int $numWorkers = 4,
        ?WorkerOptions $options = null,
        ?string $heartbeatDir = null
    ) {
        $this->driver = $driver;
        $this->queues = (array)$queues;
        $this->numWorkers = max(1, $numWorkers);
        $this->options = $options ?? new WorkerOptions();
        $this->heartbeatDir = $heartbeatDir ?? sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'eidcloud_job_workers';

        if (!is_dir($this->heartbeatDir)) {
            @mkdir($this->heartbeatDir, 0777, true);
        }

        $this->registerSignals();
    }

    private function registerSignals(): void
    {
        if (function_exists('pcntl_signal')) {
            pcntl_async_signals(true);
            pcntl_signal(SIGTERM, fn() => $this->shutdown());
            pcntl_signal(SIGINT, fn() => $this->shutdown());
        }
    }

    public function shutdown(): void
    {
        $this->shouldStop = true;
    }

    /**
     * Start the multi-process worker pool.
     * Uses pcntl_fork() on POSIX systems or background process runner / synchronous fallback on Windows.
     */
    public function start(callable $onTick = null): void
    {
        if (function_exists('pcntl_fork')) {
            $this->startPcntlPool($onTick);
        } else {
            $this->startProcessPool($onTick);
        }
    }

    /**
     * POSIX multi-process worker pooling using pcntl_fork.
     */
    private function startPcntlPool(?callable $onTick): void
    {
        $activePids = [];

        for ($i = 0; $i < $this->numWorkers; $i++) {
            $pid = pcntl_fork();
            if ($pid === -1) {
                throw new \RuntimeException("Failed to fork worker process #{$i}");
            }

            if ($pid === 0) {
                // Child process
                $workerId = "worker-child-{$i}-" . getmypid();
                $hbFile = $this->heartbeatDir . DIRECTORY_SEPARATOR . "{$workerId}.json";
                $worker = new Worker($this->driver, $this->queues, $this->options, $workerId, $hbFile);
                $worker->work();
                @unlink($hbFile);
                exit(0);
            } else {
                // Parent tracks child
                $activePids[$pid] = $i;
            }
        }

        // Parent supervisor loop
        while (!empty($activePids) && !$this->shouldStop) {
            if ($onTick) {
                $onTick($this->getHeartbeats());
            }

            // Check if any child died and restart it if pool should keep running
            foreach ($activePids as $pid => $workerIndex) {
                $res = pcntl_waitpid($pid, $status, WNOHANG);
                if ($res === $pid) {
                    unset($activePids[$pid]);

                    // Respawn worker if not shutting down
                    if (!$this->shouldStop && !$this->options->stopWhenEmpty) {
                        $newPid = pcntl_fork();
                        if ($newPid === 0) {
                            $workerId = "worker-child-{$workerIndex}-" . getmypid();
                            $hbFile = $this->heartbeatDir . DIRECTORY_SEPARATOR . "{$workerId}.json";
                            $worker = new Worker($this->driver, $this->queues, $this->options, $workerId, $hbFile);
                            $worker->work();
                            @unlink($hbFile);
                            exit(0);
                        } else {
                            $activePids[$newPid] = $workerIndex;
                        }
                    }
                }
            }

            usleep(200000);
        }

        // Graceful termination: SIGTERM all active children
        foreach (array_keys($activePids) as $pid) {
            if (function_exists('posix_kill')) {
                posix_kill($pid, SIGTERM);
            }
        }

        // Wait for all to finish
        foreach (array_keys($activePids) as $pid) {
            pcntl_waitpid($pid, $status);
        }
    }

    /**
     * Cross-platform (Windows & CLI) process supervisor using proc_open background processes.
     */
    private function startProcessPool(?callable $onTick): void
    {
        $scriptPath = realpath(__DIR__ . '/../../bin/eidcloud-job');
        if (!$scriptPath) {
            // In unit testing, run sequentially in-process
            $worker = new Worker($this->driver, $this->queues, $this->options);
            $worker->work();
            return;
        }

        $processes = [];
        $phpBinary = PHP_BINARY;
        $queueArg = implode(',', $this->queues);

        for ($i = 0; $i < $this->numWorkers; $i++) {
            $workerId = "worker-proc-{$i}-" . uniqid();
            $hbFile = $this->heartbeatDir . DIRECTORY_SEPARATOR . "{$workerId}.json";

            $cmd = sprintf(
                '"%s" "%s" work --queue=%s --single-worker --worker-id=%s --heartbeat-file="%s"',
                $phpBinary,
                $scriptPath,
                escapeshellarg($queueArg),
                escapeshellarg($workerId),
                escapeshellarg($hbFile)
            );

            if ($this->options->stopWhenEmpty) {
                $cmd .= ' --stop-when-empty';
            }

            $desc = [
                0 => ['pipe', 'r'],
                1 => ['pipe', 'w'],
                2 => ['pipe', 'w'],
            ];

            $proc = proc_open($cmd, $desc, $pipes);
            if (is_resource($proc)) {
                $processes[$i] = [
                    'proc' => $proc,
                    'pipes' => $pipes,
                    'workerId' => $workerId,
                    'hbFile' => $hbFile,
                    'cmd' => $cmd,
                ];
            }
        }

        while (!empty($processes) && !$this->shouldStop) {
            if ($onTick) {
                $onTick($this->getHeartbeats());
            }

            foreach ($processes as $i => $data) {
                $status = proc_get_status($data['proc']);
                if (!$status['running']) {
                    fclose($data['pipes'][0]);
                    fclose($data['pipes'][1]);
                    fclose($data['pipes'][2]);
                    proc_close($data['proc']);
                    @unlink($data['hbFile']);
                    unset($processes[$i]);

                    // Respawn worker if not terminating
                    if (!$this->shouldStop && !$this->options->stopWhenEmpty) {
                        $workerId = "worker-proc-{$i}-" . uniqid();
                        $hbFile = $this->heartbeatDir . DIRECTORY_SEPARATOR . "{$workerId}.json";
                        $cmd = sprintf(
                            '"%s" "%s" work --queue=%s --single-worker --worker-id=%s --heartbeat-file="%s"',
                            $phpBinary,
                            $scriptPath,
                            escapeshellarg($queueArg),
                            escapeshellarg($workerId),
                            escapeshellarg($hbFile)
                        );
                        $desc = [
                            0 => ['pipe', 'r'],
                            1 => ['pipe', 'w'],
                            2 => ['pipe', 'w'],
                        ];
                        $newProc = proc_open($cmd, $desc, $newPipes);
                        if (is_resource($newProc)) {
                            $processes[$i] = [
                                'proc' => $newProc,
                                'pipes' => $newPipes,
                                'workerId' => $workerId,
                                'hbFile' => $hbFile,
                                'cmd' => $cmd,
                            ];
                        }
                    }
                }
            }

            usleep(250000);
        }

        // Close remaining processes
        foreach ($processes as $data) {
            @proc_terminate($data['proc']);
            @fclose($data['pipes'][0]);
            @fclose($data['pipes'][1]);
            @fclose($data['pipes'][2]);
            @proc_close($data['proc']);
            @unlink($data['hbFile']);
        }
    }

    /**
     * Inspect active workers from heartbeats.
     *
     * @return array<int, array<string, mixed>>
     */
    public function getHeartbeats(): array
    {
        $files = @scandir($this->heartbeatDir) ?: [];
        $now = time();
        $workers = [];

        foreach ($files as $file) {
            if ($file === '.' || $file === '..' || !str_ends_with($file, '.json')) {
                continue;
            }

            $path = $this->heartbeatDir . DIRECTORY_SEPARATOR . $file;
            $content = @file_get_contents($path);
            if (!$content) {
                continue;
            }

            $data = @json_decode($content, true);
            if (is_array($data)) {
                $lastSeen = (int)($data['time'] ?? 0);
                // Worker is considered dead if heartbeat older than 15s
                $data['healthy'] = ($now - $lastSeen) <= 15;
                $data['age_seconds'] = $now - $lastSeen;
                $workers[] = $data;
            }
        }

        return $workers;
    }

    public function getHeartbeatDir(): string
    {
        return $this->heartbeatDir;
    }
}
