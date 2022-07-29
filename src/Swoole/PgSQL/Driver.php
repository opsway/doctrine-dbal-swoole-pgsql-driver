<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\DBAL\Swoole\PgSQL;

use Doctrine\DBAL\Driver\AbstractPostgreSQLDriver;
use OpsWay\Doctrine\DBAL\Swoole\PgSQL\Exception\ConnectionException;
use OpsWay\Doctrine\DBAL\Swoole\PgSQL\Exception\DriverException;
use Swoole\Coroutine\PostgreSQL;

use function array_key_exists;
use function implode;
use function sprintf;

use const FILTER_VALIDATE_BOOLEAN;

/** @psalm-suppress UndefinedClass, DeprecatedInterface, MissingDependency */
final class Driver extends AbstractPostgreSQLDriver
{
    public function __construct(private ?ConnectionPoolInterface $pool = null)
    {
    }

    /**
     * {@inheritdoc}
     *
     * @param string|null $username
     * @param string|null $password
     */
    public function connect(array $params, $username = null, $password = null, array $driverOptions = []) : Connection
    {
        if (! $this->pool instanceof ConnectionPoolInterface) {
            throw new DriverException('Connection pull should be initialized');
        }
        $retryMaxAttempts   = (int) ($params['retry']['maxAttempts'] ?? 1);
        $retryDelay         = (int) ($params['retry']['delay'] ?? 0);
        $connectionDelay    = (int) ($params['connectionDelay'] ?? 0);
        $usePool            =  filter_var($params['useConnectionPool'], FILTER_VALIDATE_BOOLEAN);
        $connectConstructor = $usePool ? null : static fn() : PostgreSQL => self::createConnection(self::generateDSN($params));

        return new Connection($this->pool, $retryDelay, $retryMaxAttempts, $connectionDelay, $connectConstructor);
    }

    /**
     * Create new connection for pool
     *
     * @throws ConnectionException
     */
    public static function createConnection(string $dsn) : PostgreSQL
    {
        $pgsql = new PostgreSQL();
        if (! $pgsql->connect($dsn)) {
            throw new ConnectionException(sprintf('Failed to connect: %s', (string) ($pgsql->error ?? 'Unknown')));
        }

        return $pgsql;
    }

    /**
     * @deprecated
     */
    public function getName() : string
    {
        return 'swoole_pgsql';
    }

    /**
     * Generate DSN using passed params
     */
    public static function generateDSN(array $params) : string
    {
        if (array_key_exists('url', $params)) {
            return (string) $params['url'];
        }

        return implode(';', [
            'host=' . (string) ($params['host'] ?? '127.0.0.1'),
            'port=' . (string) ($params['port'] ?? '5432'),
            'dbname=' . (string) ($params['dbname'] ?? 'postgres'),
            'user=' . (string) ($params['user'] ?? 'postgres'),
            'password=' . (string) ($params['password'] ?? 'postgres'),
        ]);
    }
}
