<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\DBAL\Swoole\PgSQL;

use Swoole\Timer;
use Swoole\ConnectionPool;
use Swoole\Coroutine as Co;
use Swoole\Coroutine\Channel;

use function in_array;
use function array_filter;

/** @psalm-suppress MissingDependency, UndefinedClass */
class DownscaleableConnectionPool extends ConnectionPool implements ConnectionPullInterface
{
    private const DEFAULT_POOL_SIZE        = 8;
    private const DOWNSCALE_TICK_FREQUENCY = 36000000; // 1 hour in milliseconds

    private Channel $connectsMap;
    private int $downscaleTimerId = -1;

    /** connectsMap should be initialized in coroutines */
    private bool $inited = false;

    public function __construct(
        callable $constructor,
        ?int $size = null,
        ?string $proxy = null,
        private ?int $tickFrequency = null
    ) {
        $this->connectsMap = new Channel(1);

        parent::__construct($constructor, $size ?? self::DEFAULT_POOL_SIZE, $proxy);
    }

    public function get(float $timeout = -1) : ConnectionWrapperInterface|bool|null
    {
        $this->init();
        /** @psalm-var ConnectionWrapperInterface|null $connection */
        $connection = parent::get($timeout);
        if ($connection instanceof ConnectionWrapperInterface) {
            /** @psalm-var string[] $map */
            $map = $this->connectsMap->pop();
            $map = array_filter($map, fn(string $id) : bool => $id !== $connection->id());
            $this->connectsMap->push($map);
        }

        return $connection;
    }

    public function removeConnect(?ConnectionWrapperInterface $connection) : void
    {
        $this->init();
        if (! $connection instanceof ConnectionWrapperInterface) {
            return;
        }
        /** @psalm-var string[] $map */
        $map = $this->connectsMap->pop();
        /** if connect presents in map, it`s attempt to remove previously returned connect */
        if (! in_array($connection->id(), $map) && $this->num > 0) {
            $this->num--;
        }
        $connection->__destruct();
        $this->connectsMap->push($map);
    }

    /** @param ConnectionWrapperInterface|null $connection */
    public function put($connection, bool $updateLastInteraction = true) : void
    {
        $this->init();
        if (! $connection instanceof ConnectionWrapperInterface) {
            return;
        }
        if ($updateLastInteraction) {
            $connection->updateLastInteraction();
        }
        if (! $connection->isReusable()) {
            $this->removeConnect($connection);

            return;
        }
        /** @psalm-var string[] $map */
        $map = $this->connectsMap->pop();
        if (! in_array($connection->id(), $map)) {
            $map[] = $connection->id();
            parent::put($connection);
        }
        $this->connectsMap->push($map);
    }

    public function close() : void
    {
        if ($this->downscaleTimerId > 0) {
            Timer::clear($this->downscaleTimerId);
        }

        parent::close();
    }

    private function init() : void
    {
        if ($this->inited) {
            return;
        }
        $this->connectsMap->push([]);
        $this->inited = true;

        // Prevent running timer for CLI commands
        // But run for HTTP server and Message workers (coroutine starts with 2)
        if (Co::getCid() > 1) {
            $this->downscaleTimerId = (int) Timer::tick(
                $this->tickFrequency ?? self::DOWNSCALE_TICK_FREQUENCY,
                fn() => $this->downscale()
            );
        }
    }

    public function downscale() : void
    {
        /** @var array $map */
        $map = $this->connectsMap->pop();
        $this->connectsMap->push($map);
        if ($this->num === 0) {
            return;
        }
        $step                = $this->num;
        $upToDateConnections = [];
        /** downscaling overdue ttl connections */
        while ($step > 0) {
            $step--;
            $connection = $this->get(1);
            if (! $connection instanceof ConnectionWrapperInterface) {
                continue;
            }
            if (! $connection->isUptoDate() || ! $connection->isReusable()) {
                $this->removeConnect($connection);

                continue;
            }
            $upToDateConnections[] = $connection;
        }
        foreach ($upToDateConnections as $connection) {
            $this->put($connection, false);
        }
        unset($upToDateConnections);
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
}
