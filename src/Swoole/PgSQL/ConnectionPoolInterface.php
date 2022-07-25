<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\DBAL\Swoole\PgSQL;

use Swoole\Coroutine\PostgreSQL;

interface ConnectionPoolInterface
{
    /** @psalm-return array{0 : PostgreSQL|null, 1 : ConnectionStats|null } */
    public function get(float $timeout = -1) : array;

    public function put(PostgreSQL $connection) : void;

    public function capacity() : int;

    public function close() : void;
}
