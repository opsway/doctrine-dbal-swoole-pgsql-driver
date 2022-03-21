<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\DBAL\Swoole\PgSQL;

use Psr\Container\ContainerInterface;

/**
 * @psalm-consistent-constructor
 */
class ConnectionPullFactory
{
    // TTL in milliseconds
    public const DEFAULT_CONNECTION_TTL = 60000;
    // Allowed queries per connection
    public const DEFAULT_USED_TIMES = 0;

    /** @var string */
    private string $configKey;

    /**
     * @param string $configKey
     */
    public function __construct($configKey = 'orm_default')
    {
        $this->configKey = $configKey;
    }

    /**
     * @return mixed
     */
    public function __invoke(ContainerInterface $container)
    {
        /** @psalm-suppress MixedAssignment */
        $config = $container->has('config') ? $container->get('config') : [];
        /**
         * @psalm-suppress MixedAssignment
         * @psalm-suppress MixedArrayAccess
         */
        $params = $config['doctrine']['connection'][$this->configKey]['params'] ?? [];

        /**
         * @var int|null $pullSize
         * @psalm-suppress MixedArrayAccess
         */
        $pullSize = $params['poolSize'] ?? null;
        /**
         * @var int|string|null $tickFrequency
         * @psalm-suppress MixedArrayAccess
         */
        $tickFrequency = $params['tickFrequency'] ?? null;
        /** @psalm-suppress MixedArrayAccess */
        $connectionTtl = (int) ($params['connectionTtl'] ?? self::DEFAULT_CONNECTION_TTL) / 1000;

        /**
         * @psalm-suppress MissingDependency
         * @psalm-suppress MixedArgument
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
