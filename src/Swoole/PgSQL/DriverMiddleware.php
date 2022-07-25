<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\DBAL\Swoole\PgSQL;

use Doctrine\DBAL\Driver as DriverInterface;
use Doctrine\DBAL\Driver\Middleware as MiddlewareInterface;

final class DriverMiddleware implements MiddlewareInterface
{
    public function __construct(private ConnectionPoolInterface $connectionPool)
    {
    }

    public function wrap(DriverInterface $driver) : DriverInterface
    {
        return new Driver($this->connectionPool);
    }
}
