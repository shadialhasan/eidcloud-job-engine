<?php

declare(strict_types=1);

namespace EidCloud\JobEngine\Contracts;

interface JobInterface
{
    /**
     * Execute the job logic with the given payload parameters.
     *
     * @param array<string, mixed> $payload
     * @return mixed
     */
    public function handle(array $payload): mixed;
}
