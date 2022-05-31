<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\DBAL\Swoole\PgSQL;

/**
 * @psalm-type ConnectionPullFactoryConfig = array{
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
class ConnectionPullFactory
{
    // TTL in milliseconds
    public const DEFAULT_CONNECTION_TTL = 60000;
    // Allowed queries per connection
    public const DEFAULT_USED_TIMES = 0;

    /**
     * @psalm-param ConnectionPullFactoryConfig $params
     */
    public function __invoke(array $params) : DownscaleableConnectionPool
    {
        /**
         * @var int|null $pullSize
         */
        $pullSize = $params['poolSize'] ?? null;
        /**
         * @var int|string|null $tickFrequency
         */
        $tickFrequency = $params['tickFrequency'] ?? null;
        /** @psalm-suppress RedundantCastGivenDocblockType */
        $connectionTtl = (int) ($params['connectionTtl'] ?? self::DEFAULT_CONNECTION_TTL) / 1000;

        /**
         * @psalm-suppress MissingDependency
         * @psalm-suppress MixedArgument
         * @psalm-suppress RedundantCastGivenDocblockType
         * @psalm-suppress RedundantCast
         */
        return new DownscaleableConnectionPool(
            static fn() : PsqlConnectionWrapper => Driver::createConnection(
                Driver::generateDSN($params),
                (int) $connectionTtl, // TTL in seconds
                (int) ($params['usedTimes'] ?? self::DEFAULT_USED_TIMES)
            ),
            $pullSize,
            tickFrequency: $tickFrequency ? (int) $tickFrequency : null
        );
    }
}
