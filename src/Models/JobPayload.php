<?php

declare(strict_types=1);

namespace EidCloud\JobEngine\Models;

use JsonSerializable;

class JobPayload implements JsonSerializable
{
    public const PRIORITY_HIGH = 100;
    public const PRIORITY_DEFAULT = 50;
    public const PRIORITY_LOW = 10;

    public const STATUS_PENDING = 'pending';
    public const STATUS_PROCESSING = 'processing';
    public const STATUS_COMPLETED = 'completed';
    public const STATUS_FAILED = 'failed';
    public const STATUS_DEAD = 'dead';

    private string $id;
    private string $jobClass;
    private array $data;
    private string $queue;
    private int $priority;
    private int $attempts;
    private int $maxAttempts;
    private int $timeout;
    private int $retryDelaySeconds;
    private bool $exponentialBackoff;
    private array $chain;
    private ?string $parentId;
    private ?string $rateLimitKey;
    private ?int $availableAt;
    private int $createdAt;
    private ?int $startedAt;
    private ?int $completedAt;
    private ?string $lastError;

    /**
     * @param array<string, mixed> $data
     * @param array<int, array<string, mixed>> $chain
     */
    public function __construct(
        string $jobClass,
        array $data = [],
        string $queue = 'default',
        int $priority = self::PRIORITY_DEFAULT,
        int $maxAttempts = 3,
        int $timeout = 60,
        int $retryDelaySeconds = 2,
        bool $exponentialBackoff = true,
        array $chain = [],
        ?string $parentId = null,
        ?string $rateLimitKey = null,
        ?int $availableAt = null,
        ?string $id = null,
        int $attempts = 0,
        ?int $createdAt = null,
        ?int $startedAt = null,
        ?int $completedAt = null,
        ?string $lastError = null
    ) {
        $this->id = $id ?? self::generateUuid();
        $this->jobClass = $jobClass;
        $this->data = $data;
        $this->queue = $queue;
        $this->priority = $priority;
        $this->maxAttempts = $maxAttempts;
        $this->timeout = $timeout;
        $this->retryDelaySeconds = $retryDelaySeconds;
        $this->exponentialBackoff = $exponentialBackoff;
        $this->chain = $chain;
        $this->parentId = $parentId;
        $this->rateLimitKey = $rateLimitKey;
        $this->createdAt = $createdAt ?? time();
        $this->availableAt = $availableAt ?? $this->createdAt;
        $this->attempts = $attempts;
        $this->startedAt = $startedAt;
        $this->completedAt = $completedAt;
        $this->lastError = $lastError;
    }

    public static function generateUuid(): string
    {
        $data = random_bytes(16);
        $data[6] = chr(ord($data[6]) & 0x0f | 0x40); // version 4
        $data[8] = chr(ord($data[8]) & 0x3f | 0x80); // variant RFC 4122
        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
    }

    public function getId(): string
    {
        return $this->id;
    }

    public function getJobClass(): string
    {
        return $this->jobClass;
    }

    public function getData(): array
    {
        return $this->data;
    }

    public function setData(array $data): self
    {
        $this->data = $data;
        return $this;
    }

    public function getQueue(): string
    {
        return $this->queue;
    }

    public function setQueue(string $queue): self
    {
        $this->queue = $queue;
        return $this;
    }

    public function getPriority(): int
    {
        return $this->priority;
    }

    public function setPriority(int $priority): self
    {
        $this->priority = $priority;
        return $this;
    }

    public function getAttempts(): int
    {
        return $this->attempts;
    }

    public function incrementAttempts(): int
    {
        return ++$this->attempts;
    }

    public function getMaxAttempts(): int
    {
        return $this->maxAttempts;
    }

    public function getTimeout(): int
    {
        return $this->timeout;
    }

    public function getRetryDelaySeconds(): int
    {
        return $this->retryDelaySeconds;
    }

    public function hasExponentialBackoff(): bool
    {
        return $this->exponentialBackoff;
    }

    public function calculateNextRetryDelay(): int
    {
        if (!$this->exponentialBackoff) {
            return $this->retryDelaySeconds;
        }

        // Exponential backoff: baseDelay * 2^(attempts - 1)
        // e.g. base 2: attempt 1 -> 2s, attempt 2 -> 4s, attempt 3 -> 8s
        $power = max(0, $this->attempts - 1);
        $delay = $this->retryDelaySeconds * (2 ** $power);
        return min($delay, 3600); // Cap at 1 hour
    }

    public function getChain(): array
    {
        return $this->chain;
    }

    public function hasChain(): bool
    {
        return !empty($this->chain);
    }

    public function shiftNextChainedJob(): ?JobPayload
    {
        if (empty($this->chain)) {
            return null;
        }

        $nextSpec = array_shift($this->chain);
        $jobClass = $nextSpec['jobClass'] ?? '';
        $data = $nextSpec['data'] ?? [];
        $queue = $nextSpec['queue'] ?? $this->queue;
        $priority = $nextSpec['priority'] ?? $this->priority;

        return new JobPayload(
            jobClass: $jobClass,
            data: $data,
            queue: $queue,
            priority: $priority,
            maxAttempts: $nextSpec['maxAttempts'] ?? $this->maxAttempts,
            timeout: $nextSpec['timeout'] ?? $this->timeout,
            retryDelaySeconds: $nextSpec['retryDelaySeconds'] ?? $this->retryDelaySeconds,
            exponentialBackoff: $nextSpec['exponentialBackoff'] ?? $this->exponentialBackoff,
            chain: $this->chain, // pass down remaining chain
            parentId: $this->id
        );
    }

    public function appendChain(string $jobClass, array $data = [], ?string $queue = null, ?int $priority = null): self
    {
        $this->chain[] = [
            'jobClass' => $jobClass,
            'data' => $data,
            'queue' => $queue ?? $this->queue,
            'priority' => $priority ?? $this->priority,
        ];
        return $this;
    }

    public function getParentId(): ?string
    {
        return $this->parentId;
    }

    public function getRateLimitKey(): ?string
    {
        return $this->rateLimitKey;
    }

    public function setRateLimitKey(?string $key): self
    {
        $this->rateLimitKey = $key;
        return $this;
    }

    public function getAvailableAt(): int
    {
        return $this->availableAt ?? $this->createdAt;
    }

    public function setAvailableAt(int $timestamp): self
    {
        $this->availableAt = $timestamp;
        return $this;
    }

    public function isAvailable(): bool
    {
        return time() >= $this->getAvailableAt();
    }

    public function getCreatedAt(): int
    {
        return $this->createdAt;
    }

    public function getStartedAt(): ?int
    {
        return $this->startedAt;
    }

    public function setStartedAt(?int $startedAt): self
    {
        $this->startedAt = $startedAt;
        return $this;
    }

    public function getCompletedAt(): ?int
    {
        return $this->completedAt;
    }

    public function setCompletedAt(?int $completedAt): self
    {
        $this->completedAt = $completedAt;
        return $this;
    }

    public function getLastError(): ?string
    {
        return $this->lastError;
    }

    public function setLastError(?string $error): self
    {
        $this->lastError = $error;
        return $this;
    }

    public function canRetry(): bool
    {
        return $this->attempts < $this->maxAttempts;
    }

    public function toArray(): array
    {
        return [
            'id' => $this->id,
            'jobClass' => $this->jobClass,
            'data' => $this->data,
            'queue' => $this->queue,
            'priority' => $this->priority,
            'attempts' => $this->attempts,
            'maxAttempts' => $this->maxAttempts,
            'timeout' => $this->timeout,
            'retryDelaySeconds' => $this->retryDelaySeconds,
            'exponentialBackoff' => $this->exponentialBackoff,
            'chain' => $this->chain,
            'parentId' => $this->parentId,
            'rateLimitKey' => $this->rateLimitKey,
            'availableAt' => $this->availableAt,
            'createdAt' => $this->createdAt,
            'startedAt' => $this->startedAt,
            'completedAt' => $this->completedAt,
            'lastError' => $this->lastError,
        ];
    }

    public static function fromArray(array $data): self
    {
        return new self(
            jobClass: $data['jobClass'] ?? '',
            data: $data['data'] ?? [],
            queue: $data['queue'] ?? 'default',
            priority: (int)($data['priority'] ?? self::PRIORITY_DEFAULT),
            maxAttempts: (int)($data['maxAttempts'] ?? 3),
            timeout: (int)($data['timeout'] ?? 60),
            retryDelaySeconds: (int)($data['retryDelaySeconds'] ?? 2),
            exponentialBackoff: (bool)($data['exponentialBackoff'] ?? true),
            chain: $data['chain'] ?? [],
            parentId: $data['parentId'] ?? null,
            rateLimitKey: $data['rateLimitKey'] ?? null,
            availableAt: isset($data['availableAt']) ? (int)$data['availableAt'] : null,
            id: $data['id'] ?? null,
            attempts: (int)($data['attempts'] ?? 0),
            createdAt: isset($data['createdAt']) ? (int)$data['createdAt'] : null,
            startedAt: isset($data['startedAt']) ? (int)$data['startedAt'] : null,
            completedAt: isset($data['completedAt']) ? (int)$data['completedAt'] : null,
            lastError: $data['lastError'] ?? null
        );
    }

    public function toJson(): string
    {
        return json_encode($this->toArray(), JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES);
    }

    public static function fromJson(string $json): self
    {
        $decoded = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        return self::fromArray($decoded);
    }

    public function jsonSerialize(): mixed
    {
        return $this->toArray();
    }
}
