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
    /** @psalm-var WeakMap<PostgreSQL, ConnectionStats> $map */
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
        /** @psalm-suppress PropertyTypeCoercion */
        $this->map = new WeakMap();
    }

    /** @psalm-return array{PostgreSQL|null, ConnectionStats|null } */
    public function get(float $timeout = -1) : array
    {
        /** Pool was closed */
        if (! $this->map || ! $this->pool) {
            throw new DriverException('ConnectionPool was closed');
        }
        /** @var PostgreSQL|null $connection */
        $connection = $this->pool->pop($timeout);
        if (! $connection instanceof PostgreSQL) {
            /** try to fill pull with new connect */
            $this->make();
            /** @var PostgreSQL|null $connection */
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
        /** Pool was closed */
        if (! $this->map || ! $this->pool) {
            return;
        }
        if (! $this->map->offsetExists($connection)) {
            return;
        }
        if ($this->pool->isFull()) {
            $this->remove($connection);

            return;
        }
        $stats = $this->map[$connection] ?? null;
        if (! $stats || $stats->isOverdue()) {
            $this->remove($connection);

            return;
        }
        $this->pool->push($connection);
    }

    public function close() : void
    {
        /** Pool was closed */
        if (! $this->map || ! $this->pool) {
            return;
        }
        $this->pool->close();
        $this->pool = null;
        $this->map  = null;
    }

    public function capacity() : int
    {
        return (int) $this->map?->count();
    }

    public function length() : int
    {
        return (int) $this->pool?->length();
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
        /** Pool was closed */
        if (! $this->map || ! $this->pool) {
            return;
        }
        $this->map->offsetUnset($connection);
        unset($connection);
    }

    private function make() : void
    {
        /** Pool was closed */
        if (! $this->map || ! $this->pool) {
            return;
        }
        if ($this->pool->capacity === $this->capacity()) {
            return;
        }
        try {
            /** @var PostgreSQL $connection */
            $connection = ($this->constructor)();
        } catch (Throwable) {
            throw new Exception('Could not initialize connection with constructor');
        }
        $this->map[$connection] = new ConnectionStats(time(), 1, $this->connectionTtl, $this->connectionUseLimit);
        /** @psalm-suppress PossiblyNullReference */
        $this->pool->push($connection);
    }
}
