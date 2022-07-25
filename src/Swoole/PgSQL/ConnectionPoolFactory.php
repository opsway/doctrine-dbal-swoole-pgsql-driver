<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\DBAL\Swoole\PgSQL;

use Swoole\Coroutine\PostgreSQL;

/**
 * @psalm-type ConnectionPoolFactoryConfig = array{
 *     dbname: 'mydb',
 *     user: 'user',
 *     password: 'secret',
 *     host: 'localhost',
 *     driverClass: class-string<\OpsWay\Doctrine\DBAL\Swoole\PgSQL\Driver>,
 *     poolSize: 5,
 *     tickFrequency: 60000,
 *     connectionTtl: 60000,
 *     usedTimes: 100,
 *     retry: array{
 *         max_attempts: 2,
 *         delay: 1,
 *     },
 * }
 * @psalm-suppress MissingDependency, UndefinedClass
 */
class ConnectionPoolFactory
{
    // Allowed IDLE time in seconds
    public const DEFAULT_CONNECTION_TTL = 60;
    // Allowed queries per connection
    public const DEFAULT_USAGE_LIMIT = 0;

    /**
     * @psalm-param ConnectionPoolFactoryConfig $params
     */
    public function __invoke(array $params) : ConnectionPoolInterface
    {
        /**
         * @var int|null $pullSize
         */
        $pullSize = $params['poolSize'] ?? null;
        /** @var int|string $usageLimit */
        $usageLimit = $params['usedTimes'] ?? self::DEFAULT_USAGE_LIMIT;

        /** @psalm-suppress RedundantCastGivenDocblockType */
        $connectionTtl = (int) ($params['connectionTtl'] ?? self::DEFAULT_CONNECTION_TTL);

        /**
         * @psalm-suppress MissingDependency
         * @psalm-suppress MixedArgument
         * @psalm-suppress RedundantCastGivenDocblockType
         * @psalm-suppress RedundantCast
         */
        return new ConnectionPool(
            static fn() : PostgreSQL => Driver::createConnection(Driver::generateDSN($params)),
            $pullSize,
            $connectionTtl,
            (int) $usageLimit
        );
    }
}
