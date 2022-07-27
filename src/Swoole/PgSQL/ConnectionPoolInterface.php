<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\DBAL\Swoole\PgSQL;

use Swoole\Coroutine\PostgreSQL;

interface ConnectionPoolInterface
{
    /** @psalm-return array{PostgreSQL|null, ConnectionStats|null } */
    public function get(float $timeout = -1) : array;

    public function put(PostgreSQL $connection) : void;

    public function capacity() : int;

    public function length() : int;

    public function close() : void;
}
