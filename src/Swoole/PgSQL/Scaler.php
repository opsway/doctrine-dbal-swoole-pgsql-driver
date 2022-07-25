<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\DBAL\Swoole\PgSQL;

use Swoole\Coroutine\PostgreSQL;
use Swoole\Timer;

use function array_map;

class Scaler
{
    private const DOWNSCALE_TICK_FREQUENCY = 36000000; // 1 hour in milliseconds

    private ?int $timerId = null;

    public function __construct(private ConnectionPoolInterface $pool, private ?int $tickFrequency)
    {
    }

    public function run() : void
    {
        if ($this->timerId) {
            return;
        }
        $this->timerId = Timer::tick(
            $this->tickFrequency ?? self::DOWNSCALE_TICK_FREQUENCY,
            fn() => $this->downscale()
        ) ?: null;
    }

    private function downscale() : void
    {
        $poolCapacity = $this->pool->capacity();
        /** @psalm-var  PostgreSQL[] $connections */
        $connections = [];
        while ($poolCapacity > 0) {
            /** @psalm-suppress UnusedVariable */
            [$connection, $connectionStats] = $this->pool->get();
            /** connection never null if poll capacity > 0 */
            if (! $connection) {
                return;
            }
            $connections[] = $connection;
            $poolCapacity--;
        }
        array_map(fn(PostgreSQL $connection) => $this->pool->put($connection), $connections);
    }

    public function close() : void
    {
        if (! $this->timerId) {
            return;
        }
        Timer::clear($this->timerId);
    }
}
