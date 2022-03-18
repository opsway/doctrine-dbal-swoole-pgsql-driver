<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\DBAL\Swoole\PgSQL;

use Throwable;
use Swoole\Coroutine\PostgreSQL;
use Doctrine\DBAL\Exception as DBALException;
use Doctrine\DBAL\Driver\AbstractPostgreSQLDriver;
use OpsWay\Doctrine\DBAL\Swoole\PgSQL\Exception\DriverException;
use OpsWay\Doctrine\DBAL\Swoole\PgSQL\Exception\ConnectionException;

use function abs;
use function trim;
use function defer;
use function usleep;
use function implode;
use function sprintf;
use function is_resource;
use function array_key_exists;

/** @psalm-suppress UndefinedClass, DeprecatedInterface, MissingDependency */
final class Driver extends AbstractPostgreSQLDriver
{
    public function __construct(private ?ConnectionPullInterface $pool = null)
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
        $pool = $this->pool;
        if (! $pool instanceof ConnectionPullInterface) {
            throw new DriverException('Connection pull should be initialized');
        }
        $retryMaxAttempts = (int) ($params['retry']['max_attempts'] ?? 1);
        $retryDelay       = (int) ($params['retry']['delay'] ?? 0);
        $lastException    = null;
        $connect          = null;
        for ($i = 0; $i < $retryMaxAttempts; $i++) {
            try {
                /** @psalm-suppress MissingDependency */
                $connect = $pool->get(2);
                if (! $connect instanceof ConnectionWrapperInterface) {
                    throw new DriverException('No connect available in pull');
                }
                /** @var resource|bool $query */
                $query        = $connect->query('SELECT 1');
                $affectedRows = is_resource($query) ? $connect->affectedRows($query) : 0;
                if ($affectedRows !== 1) {
                    throw new ConnectionException('Connection ping failed. Trying reconnect (attempt ' . $i . '). Reason: ' . trim($connect->error()));
                }

                break;
            } catch (Throwable $e) {
                $errCode = '';
                if ($connect instanceof ConnectionWrapperInterface) {
                    $errCode = $connect->errorCode();
                    /** @psalm-suppress MissingDependency */
                    $pool->removeConnect($connect);
                    $connect = null;
                }
                $lastException = $e instanceof DBALException ? $e : new ConnectionException($e->getMessage(), (string) $errCode, '', (int) $e->getCode(), $e);
                /** @psalm-suppress ArgumentTypeCoercion */
                usleep(abs($retryDelay) * 1000);  // Sleep ms after failure
            }
        }
        if (! $connect instanceof ConnectionWrapperInterface) {
            $lastException instanceof Throwable ? throw $lastException : throw new ConnectionException('Connection could not be initiated');
        }

        /** @psalm-suppress MissingClosureReturnType,PossiblyNullReference,UnusedFunctionCall,UnusedVariable */
        defer(static fn() => $pool->put($connect));

        /** @psalm-suppress  PossiblyNullArgument */
        return new Connection($connect);
    }

    /**
     * Create new connection for pool
     *
     * @throws ConnectionException
     */
    public static function createConnection(string $dsn, int $ttl, int $maxUsageTimes) : PsqlConnectionWrapper
    {
        $pgsql = new PostgreSQL();
        if (! $pgsql->connect($dsn)) {
            throw new ConnectionException(sprintf('Failed to connect: %s', (string) ($pgsql->error ?? 'Unknown')));
        }

        return new PsqlConnectionWrapper($pgsql, $ttl, $maxUsageTimes);
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
