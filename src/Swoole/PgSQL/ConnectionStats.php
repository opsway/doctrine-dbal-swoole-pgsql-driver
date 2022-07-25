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
        return match (true) {
            ! $this->counterLimit && ! $this->ttl,
            $this->counterLimit && $this->counterLimit > $this->counter,
            $this->ttl && time() - $this->lastInteraction > $this->ttl => false,
            default                                                    => true
        };
    }
}
