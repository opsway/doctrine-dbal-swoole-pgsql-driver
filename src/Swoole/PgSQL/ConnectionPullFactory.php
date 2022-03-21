<?php

declare(strict_types=1);

namespace Boodmo\Marketplace\Shared\Infrastructure\Doctrine;

use OpsWay\Doctrine\DBAL\Swoole\PgSQL\DownscaleableConnectionPool;
use OpsWay\Doctrine\DBAL\Swoole\PgSQL\Driver;
use OpsWay\Doctrine\DBAL\Swoole\PgSQL\PsqlConnectionWrapper;
use Psr\Container\ContainerInterface;

/**
 * @psalm-consistent-constructor
 * @template TProduct
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
     * @psalm-return TProduct
     */
    public function __invoke(ContainerInterface $container)
    {
        $config = $container->has('config') ? $container->get('config') : [];
        $params = $config['doctrine']['connection'][$this->configKey]['params'] ?? [];

        /** @var int|null $pullSize */
        $pullSize = $params['poolSize'] ?? null;
        /** @var int|string|null $tickFrequency */
        $tickFrequency = $params['tickFrequency'] ?? null;
        $connectionTtl = (int) ($params['connectionTtl'] ?? self::DEFAULT_CONNECTION_TTL) / Units::SECONDS;

        /** @psalm-suppress MissingDependency */
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
