<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\DBAL\Swoole\PgSQL;

use function time;

class ConnectionStats
{
    public function __construct(
        public int $lastInteraction,
        public int $counter,
        private ?int $ttl,
        private ?int $counterLimit
    ) {
    }

    public function isOverdue() : bool
    {
        if (! $this->counterLimit && ! $this->ttl) {
            return false;
        }
        $counterOverflow = $this->counterLimit !== null && $this->counter > $this->counterLimit;
        $ttlOverdue      = $this->ttl !== null && time() - $this->lastInteraction > $this->ttl;

        return $counterOverflow || $ttlOverdue;
    }
}
