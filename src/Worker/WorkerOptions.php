<?php

declare(strict_types=1);

namespace EidCloud\JobEngine\Worker;

class WorkerOptions
{
    public function __construct(
        public int $sleepMicroseconds = 250000, // 250ms when idle
        public int $maxJobs = 0,               // 0 = unlimited
        public int $maxMemoryBytes = 134217728, // 128 MB default limit
        public bool $stopWhenEmpty = false,
        public int $rateLimitDelaySeconds = 1
    ) {
    }
}
