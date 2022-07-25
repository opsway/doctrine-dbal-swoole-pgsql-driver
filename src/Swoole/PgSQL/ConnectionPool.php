<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\DBAL\Swoole\PgSQL;

use Closure;
use Exception;
use OpsWay\Doctrine\DBAL\Swoole\PgSQL\Exception\DriverException;
use Swoole\Coroutine\Channel;
use Swoole\Coroutine\PostgreSQL;
use Throwable;
use WeakMap;

use function time;

final class ConnectionPool implements ConnectionPoolInterface
{
    private ?Channel $pool = null;
    private ?WeakMap $map  = null;

    public function __construct(
        private Closure $constructor,
        private int $size,
        private ?int $connectionTtl = null,
        private ?int $connectionUseLimit = null
    ) {
        if ($this->size < 0) {
            throw new DriverException('Expected, connection pull size > 0');
        }
        $this->pool = new Channel($this->size);
        $this->map  = new WeakMap();
    }

    /** @psalm-return array{0 : PostgreSQL|null, 1 : ConnectionStats|null } */
    public function get(float $timeout = -1) : array
    {
        $connection = $this->pool->pop($timeout);
        if (! $connection instanceof PostgreSQL) {
            /** try to fill pull with new connect */
            $this->make();
            $connection = $this->pool->pop($timeout);
        }
        if (! $connection instanceof PostgreSQL) {
            return [null, null];
        }

        return [
            $connection,
            $this->map[$connection] ?? throw new DriverException('Connection stats could not be empty'),
        ];
    }

    public function put(PostgreSQL $connection) : void
    {
        if (! $this->map->offsetExists($connection)) {
            return;
        }
        if ($this->pool->isFull()) {
            $this->remove($connection);

            return;
        }
        /** @psalm-var ConnectionStats|null $stats */
        $stats = $this->map[$connection] ?? null;
        if (! $stats || $stats->isOverdue()) {
            $this->remove($connection);

            return;
        }
        $this->pool->push($connection);
    }

    public function close() : void
    {
        $this->pool->close();
        $this->pool = null;
        $this->map  = null;
    }

    public function capacity() : int
    {
        return $this->pool->capacity;
    }

    /**
     * Exclude object data from doctrine cache serialization
     *
     * @see vendor/doctrine/dbal/src/Cache/QueryCacheProfile.php:127
     */
    public function __serialize() : array
    {
        return [];
    }

    /**
     * @param string $data
     */
    public function __unserialize($data) : void
    {
        // Do nothing
    }

    private function remove(PostgreSQL $connection) : void
    {
        $this->map->offsetUnset($connection);
        unset($connection);
    }

    private function make() : void
    {
        if ($this->pool->capacity === $this->map->count()) {
            return;
        }
        try {
            $connection = ($this->constructor)();
        } catch (Throwable) {
            throw new Exception('Could not initialize connection with constructor');
        }
        $this->map[$connection] = new ConnectionStats(time(), 1, $this->connectionTtl, $this->connectionUseLimit);
        $this->put($connection);
    }
}
