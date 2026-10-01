<?php

declare(strict_types=1);

namespace EidCloud\JobEngine\Drivers;

use EidCloud\JobEngine\Contracts\QueueDriverInterface;
use EidCloud\JobEngine\Exceptions\QueueDriverException;
use EidCloud\JobEngine\Models\JobPayload;

class RedisQueueDriver implements QueueDriverInterface
{
    private mixed $redis;
    private string $prefix;

    /**
     * Accepts either a \Redis instance or array configuration to connect.
     * Can connect using either ext-redis or raw sockets if ext-redis is absent.
     *
     * @param mixed $connection \Redis instance or array ['host' => '127.0.0.1', 'port' => 6379, 'password' => null, 'database' => 0]
     */
    public function __construct(mixed $connection = null, string $prefix = 'eidcloud:jobs:')
    {
        $this->prefix = $prefix;
        if ($connection instanceof \Redis) {
            $this->redis = $connection;
        } elseif (is_array($connection)) {
            $this->connectFromArray($connection);
        } elseif (class_exists(\Redis::class)) {
            $this->connectFromArray(['host' => '127.0.0.1', 'port' => 6379]);
        } else {
            // Lazy raw-socket Redis client fallback for pure PHP environments without ext-redis
            $this->redis = new PurePhpRedisClient('127.0.0.1', 6379);
        }
    }

    private function connectFromArray(array $config): void
    {
        $host = $config['host'] ?? '127.0.0.1';
        $port = (int)($config['port'] ?? 6379);
        $timeout = (float)($config['timeout'] ?? 2.5);

        if (class_exists(\Redis::class)) {
            $client = new \Redis();
            if (!@$client->connect($host, $port, $timeout)) {
                // If connection fails, keep pure php fallback client
                $this->redis = new PurePhpRedisClient($host, $port, $timeout, $config['password'] ?? null, (int)($config['database'] ?? 0));
                return;
            }
            if (!empty($config['password'])) {
                $client->auth($config['password']);
            }
            if (isset($config['database']) && $config['database'] !== 0) {
                $client->select((int)$config['database']);
            }
            $this->redis = $client;
        } else {
            $this->redis = new PurePhpRedisClient($host, $port, $timeout, $config['password'] ?? null, (int)($config['database'] ?? 0));
        }
    }

    private function k(string $name): string
    {
        return $this->prefix . $name;
    }

    public function push(JobPayload $job): string
    {
        $now = time();
        $jobId = $job->getId();
        $payloadJson = $job->toJson();

        // Store payload in a hash
        $this->redis->hSet($this->k('payloads'), $jobId, $payloadJson);

        if ($job->getAvailableAt() > $now) {
            // Delayed: sorted set scored by available timestamp
            $this->redis->zAdd($this->k('delayed'), $job->getAvailableAt(), $jobId);
        } else {
            // Pending: sorted set scored by priority DESC (higher score popped first)
            $queueKey = $this->k("queue:{$job->getQueue()}");
            $this->redis->zAdd($queueKey, $job->getPriority(), $jobId);
        }

        return $jobId;
    }

    private function migrateDelayedJobs(): void
    {
        $now = time();
        $delayedKey = $this->k('delayed');
        // Fetch jobs ready to run
        $ready = $this->redis->zRangeByScore($delayedKey, '-inf', (string)$now);
        if (empty($ready)) {
            return;
        }

        foreach ($ready as $jobId) {
            $payloadJson = $this->redis->hGet($this->k('payloads'), $jobId);
            if ($payloadJson) {
                $job = JobPayload::fromJson($payloadJson);
                $this->redis->zRem($delayedKey, $jobId);
                $queueKey = $this->k("queue:{$job->getQueue()}");
                $this->redis->zAdd($queueKey, $job->getPriority(), $jobId);
            } else {
                $this->redis->zRem($delayedKey, $jobId);
            }
        }
    }

    public function pop(array|string $queues): ?JobPayload
    {
        $this->migrateDelayedJobs();
        $queueList = (array)$queues;
        if (empty($queueList)) {
            $queueList = ['default'];
        }

        $now = time();
        foreach ($queueList as $queue) {
            $queueKey = $this->k("queue:{$queue}");
            // Highest priority first -> zRevRange 0 0
            $items = $this->redis->zRevRange($queueKey, 0, 0);
            if (empty($items)) {
                continue;
            }

            $jobId = $items[0];
            $removed = $this->redis->zRem($queueKey, $jobId);
            if ($removed <= 0) {
                // Another worker got it
                continue;
            }

            $payloadJson = $this->redis->hGet($this->k('payloads'), $jobId);
            if (!$payloadJson) {
                continue;
            }

            $job = JobPayload::fromJson($payloadJson);
            $job->setStartedAt($now);
            $job->incrementAttempts();

            // Store in processing sorted set scored by expiration (startedAt + timeout)
            $this->redis->zAdd($this->k('processing'), $now + $job->getTimeout(), $jobId);
            // Update payload in hash
            $this->redis->hSet($this->k('payloads'), $jobId, $job->toJson());

            return $job;
        }

        return null;
    }

    public function acknowledge(string $jobId): bool
    {
        $this->redis->zRem($this->k('processing'), $jobId);
        $this->redis->hDel($this->k('payloads'), $jobId);
        $this->redis->incr($this->k('stats:completed'));
        return true;
    }

    public function release(JobPayload $job, int $delaySeconds = 0): bool
    {
        $jobId = $job->getId();
        $this->redis->zRem($this->k('processing'), $jobId);

        $availableAt = time() + max(0, $delaySeconds);
        $job->setAvailableAt($availableAt);
        $this->redis->hSet($this->k('payloads'), $jobId, $job->toJson());

        if ($delaySeconds > 0) {
            $this->redis->zAdd($this->k('delayed'), $availableAt, $jobId);
        } else {
            $queueKey = $this->k("queue:{$job->getQueue()}");
            $this->redis->zAdd($queueKey, $job->getPriority(), $jobId);
        }

        return true;
    }

    public function moveToDeadLetter(JobPayload $job, string $reason = ''): bool
    {
        $jobId = $job->getId();
        $this->redis->zRem($this->k('processing'), $jobId);

        if ($reason) {
            $job->setLastError($reason);
        }

        $dlqRecord = [
            'id' => $job->getId(),
            'queue' => $job->getQueue(),
            'job_class' => $job->getJobClass(),
            'payload' => $job->toJson(),
            'failed_at' => time(),
            'attempts' => $job->getAttempts(),
            'error_message' => $job->getLastError() ?? $reason,
        ];

        $this->redis->hSet($this->k('dlq:records'), $jobId, json_encode($dlqRecord));
        $this->redis->zAdd($this->k('dlq:index'), time(), $jobId);
        $this->redis->hDel($this->k('payloads'), $jobId);

        return true;
    }

    public function getDeadLetters(int $limit = 50, int $offset = 0): array
    {
        $jobIds = $this->redis->zRevRange($this->k('dlq:index'), $offset, $offset + $limit - 1);
        if (empty($jobIds)) {
            return [];
        }

        $results = [];
        foreach ($jobIds as $id) {
            $recordJson = $this->redis->hGet($this->k('dlq:records'), $id);
            if ($recordJson) {
                $results[] = json_decode($recordJson, true);
            }
        }
        return $results;
    }

    public function replayDeadLetter(string $jobId): bool
    {
        $recordJson = $this->redis->hGet($this->k('dlq:records'), $jobId);
        if (!$recordJson) {
            return false;
        }

        $record = json_decode($recordJson, true);
        if (!isset($record['payload'])) {
            return false;
        }

        $job = JobPayload::fromJson($record['payload']);
        $job->setAvailableAt(time());
        $job->setLastError(null);

        $this->redis->zRem($this->k('dlq:index'), $jobId);
        $this->redis->hDel($this->k('dlq:records'), $jobId);

        $this->push($job);
        return true;
    }

    public function purgeDeadLetter(?string $jobId = null): int
    {
        if ($jobId !== null) {
            $this->redis->zRem($this->k('dlq:index'), $jobId);
            $del = $this->redis->hDel($this->k('dlq:records'), $jobId);
            return (int)$del;
        }

        $count = $this->redis->zCard($this->k('dlq:index'));
        $this->redis->del($this->k('dlq:index'));
        $this->redis->del($this->k('dlq:records'));
        return (int)$count;
    }

    public function size(array|string|null $queues = null): int
    {
        $this->migrateDelayedJobs();

        if ($queues === null) {
            // Find all queue:* keys
            $keys = $this->redis->keys($this->k('queue:*'));
            $total = 0;
            foreach ($keys as $k) {
                $total += (int)$this->redis->zCard($k);
            }
            return $total;
        }

        $queueList = (array)$queues;
        $total = 0;
        foreach ($queueList as $q) {
            $total += (int)$this->redis->zCard($this->k("queue:{$q}"));
        }
        return $total;
    }

    public function stats(): array
    {
        $this->migrateDelayedJobs();

        $queueKeys = $this->redis->keys($this->k('queue:*'));
        $pending = 0;
        foreach ($queueKeys as $k) {
            $pending += (int)$this->redis->zCard($k);
        }

        $processing = (int)$this->redis->zCard($this->k('processing'));
        $delayed = (int)$this->redis->zCard($this->k('delayed'));
        $deadLetters = (int)$this->redis->zCard($this->k('dlq:index'));
        $completed = (int)$this->redis->get($this->k('stats:completed'));

        return [
            'pending' => $pending,
            'processing' => $processing,
            'delayed' => $delayed,
            'completed' => $completed,
            'dead_letters' => $deadLetters,
            'total' => $pending + $delayed + $processing + $completed,
        ];
    }

    public function clear(?string $queue = null): void
    {
        if ($queue !== null) {
            $this->redis->del($this->k("queue:{$queue}"));
        } else {
            $keys = $this->redis->keys($this->k('*'));
            if (!empty($keys)) {
                $this->redis->del($keys);
            }
        }
    }
}

/**
 * Lightweight Pure PHP Redis Client using native socket streams.
 * Guarantees zero vendor dependency even if ext-redis extension is absent.
 */
class PurePhpRedisClient
{
    private mixed $socket = null;
    private string $host;
    private int $port;
    private float $timeout;
    private ?string $password;
    private int $database;

    public function __construct(string $host = '127.0.0.1', int $port = 6379, float $timeout = 2.5, ?string $password = null, int $database = 0)
    {
        $this->host = $host;
        $this->port = $port;
        $this->timeout = $timeout;
        $this->password = $password;
        $this->database = $database;
    }

    private function connect(): void
    {
        if ($this->socket && is_resource($this->socket) && !feof($this->socket)) {
            return;
        }

        $errno = 0;
        $errstr = '';
        $this->socket = @fsockopen($this->host, $this->port, $errno, $errstr, $this->timeout);

        if (!$this->socket) {
            throw new QueueDriverException("Cannot connect to Redis at {$this->host}:{$this->port}: {$errstr} ({$errno})");
        }

        stream_set_timeout($this->socket, (int)$this->timeout, (int)(($this->timeout - floor($this->timeout)) * 1000000));

        if ($this->password) {
            $this->execute(['AUTH', $this->password]);
        }
        if ($this->database > 0) {
            $this->execute(['SELECT', (string)$this->database]);
        }
    }

    public function execute(array $args): mixed
    {
        $this->connect();

        $cmd = '*' . count($args) . "\r\n";
        foreach ($args as $arg) {
            $arg = (string)$arg;
            $cmd .= '$' . strlen($arg) . "\r\n" . $arg . "\r\n";
        }

        fwrite($this->socket, $cmd);
        return $this->parseResponse();
    }

    private function parseResponse(): mixed
    {
        $line = fgets($this->socket);
        if ($line === false) {
            throw new QueueDriverException("Redis connection closed unexpectedly.");
        }

        $type = $line[0];
        $payload = substr($line, 1, -2);

        switch ($type) {
            case '+':
                return $payload; // Simple string e.g. "OK"
            case '-':
                throw new QueueDriverException("Redis error: {$payload}");
            case ':':
                return (int)$payload; // Integer
            case '$':
                $length = (int)$payload;
                if ($length === -1) {
                    return null;
                }
                $read = 0;
                $data = '';
                while ($read < $length) {
                    $chunk = fread($this->socket, $length - $read);
                    if ($chunk === false) break;
                    $data .= $chunk;
                    $read += strlen($chunk);
                }
                fread($this->socket, 2); // \r\n
                return $data;
            case '*':
                $count = (int)$payload;
                if ($count === -1) {
                    return null;
                }
                $list = [];
                for ($i = 0; $i < $count; $i++) {
                    $list[] = $this->parseResponse();
                }
                return $list;
            default:
                throw new QueueDriverException("Unknown Redis protocol response: {$line}");
        }
    }

    public function hSet(string $key, string $field, string $value): int
    {
        return (int)$this->execute(['HSET', $key, $field, $value]);
    }

    public function hGet(string $key, string $field): ?string
    {
        return $this->execute(['HGET', $key, $field]);
    }

    public function hDel(string $key, string $field): int
    {
        return (int)$this->execute(['HDEL', $key, $field]);
    }

    public function zAdd(string $key, float|int $score, string $member): int
    {
        return (int)$this->execute(['ZADD', $key, (string)$score, $member]);
    }

    public function zRem(string $key, string $member): int
    {
        return (int)$this->execute(['ZREM', $key, $member]);
    }

    public function zRevRange(string $key, int $start, int $stop): array
    {
        $res = $this->execute(['ZREVRANGE', $key, (string)$start, (string)$stop]);
        return is_array($res) ? $res : [];
    }

    public function zRangeByScore(string $key, string $min, string $max): array
    {
        $res = $this->execute(['ZRANGEBYSCORE', $key, $min, $max]);
        return is_array($res) ? $res : [];
    }

    public function zCard(string $key): int
    {
        return (int)$this->execute(['ZCARD', $key]);
    }

    public function del(string|array $keys): int
    {
        $keys = (array)$keys;
        if (empty($keys)) {
            return 0;
        }
        return (int)$this->execute(array_merge(['DEL'], $keys));
    }

    public function incr(string $key): int
    {
        return (int)$this->execute(['INCR', $key]);
    }

    public function get(string $key): ?string
    {
        return $this->execute(['GET', $key]);
    }

    public function keys(string $pattern): array
    {
        $res = $this->execute(['KEYS', $pattern]);
        return is_array($res) ? $res : [];
    }
}
