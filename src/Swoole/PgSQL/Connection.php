<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\DBAL\Swoole\PgSQL;

use Doctrine\DBAL\Driver\Connection as ConnectionInterface;
use Doctrine\DBAL\Exception as DBALException;
use Doctrine\DBAL\ParameterType;
use OpsWay\Doctrine\DBAL\SQLParserUtils;
use OpsWay\Doctrine\DBAL\Swoole\PgSQL\Exception\ConnectionException;
use OpsWay\Doctrine\DBAL\Swoole\PgSQL\Exception\DriverException;
use Swoole\Coroutine as Co;
use Swoole\Coroutine\PostgreSQL;
use Throwable;
use WeakMap;

use function defer;
use function is_resource;
use function sleep;
use function strlen;
use function substr;
use function time;
use function trim;

final class Connection implements ConnectionInterface
{
    /** @psalm-var array<int, PostgreSQL>  */
    private array $internalStorage = [];
    /** @psalm-var WeakMap<PostgreSQL, ConnectionStats> $statsStorage */
    private WeakMap $statsStorage;

    public function __construct(
        private ConnectionPoolInterface $pool,
        private int $retryDelay,
        private int $maxAttempts,
        private int $connectionDelay,
    ) {
        /** @psalm-suppress PropertyTypeCoercion */
        $this->statsStorage = new WeakMap();
        /** Outside of Coroutine Co::getCid() = -1 */
        if (Co::getCid() < 1) {
            return;
        }
        /** @psalm-suppress UnusedFunctionCall */
        defer(fn () => $this->onDefer());
    }

    /**
     * {@inheritdoc}
     *
     * @throws DriverException
     */
    public function prepare(string $sql) : Statement
    {
        $i        = 1;
        $posShift = 0;

        $phPos = SQLParserUtils::getPlaceholderPositions($sql);
        foreach ($phPos as $pos) {
            $placeholder = '$' . $i;
            $sql         = substr($sql, 0, (int) $pos + $posShift)
                . $placeholder
                . substr($sql, (int) $pos + $posShift + 1);
            $posShift   += strlen($placeholder) - 1;
            $i++;
        }
        $connection = $this->getNativeConnection();

        return new Statement($connection, $sql, $this->connectionStats($connection));
    }

    /**
     * {@inheritdoc}
     */
    public function query(string $sql) : Result
    {
        $connection = $this->getNativeConnection();
        $resource   = $connection->query($sql);
        $stats      = $this->connectionStats($connection);
        if ($stats instanceof ConnectionStats) {
            $stats->counter++;
        }

        if (! is_resource($resource)) {
            throw ConnectionException::fromConnection($connection);
        }

        return new Result($this->getNativeConnection(), $resource);
    }

    /**
     * {@inheritdoc}
     *
     * @param mixed $value
     * @param int   $type
     */
    public function quote($value, $type = ParameterType::STRING) : string
    {
        return "'" . (string) $this->getNativeConnection()->escape($value) . "'";
    }

    /**
     * {@inheritdoc}
     */
    public function exec(string $sql) : int
    {
        $connection = $this->getNativeConnection();
        $query      = $connection->query($sql);
        $stats      = $this->connectionStats($connection);
        if ($stats instanceof ConnectionStats) {
            $stats->counter++;
        }

        if (! is_resource($query)) {
            throw ConnectionException::fromConnection($this->getNativeConnection());
        }

        return (int) $this->getNativeConnection()->affectedRows($query);
    }

    /**
     * {@inheritdoc}
     *
     * @param string|null $name
     */
    public function lastInsertId($name = null) : string
    {
        $result = ! empty($name)
            ? $this->query('SELECT CURRVAL(\'' . $name . '\')')
            : $this->query('SELECT LASTVAL()');

        return (string) $result->fetchOne();
    }

    /**
     * {@inheritdoc}
     */
    public function beginTransaction() : bool
    {
        $this->getNativeConnection()->query('START TRANSACTION');

        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function commit() : bool
    {
        $this->getNativeConnection()->query('COMMIT');

        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function rollBack() : bool
    {
        $this->getNativeConnection()->query('ROLLBACK');

        return true;
    }

    public function errorCode() : int
    {
        return (int) $this->getNativeConnection()->errCode;
    }

    public function errorInfo() : string
    {
        return (string) $this->getNativeConnection()->error;
    }

    public function getNativeConnection() : PostgreSQL
    {
        $connection = $this->internalStorage[Co::getCid()] ?? null;
        if (! $connection instanceof PostgreSQL) {
            $lastException = null;
            for ($i = 0; $i < $this->maxAttempts; $i++) {
                try {
                    [$connection, $stats] = $this->pool->get($this->connectionDelay);
                    if (! $connection instanceof PostgreSQL) {
                        throw new DriverException('No connect available in pull');
                    }
                    if (! $stats instanceof ConnectionStats) {
                        throw new DriverException('Provided connect is corrupted');
                    }
                    /** @var resource|bool $query */
                    $query        = $connection->query('SELECT 1');
                    $affectedRows = is_resource($query) ? (int) $connection->affectedRows($query) : 0;
                    if ($affectedRows !== 1) {
                        $errCode = trim((string) $connection->errCode);
                        throw new ConnectionException(
                            "Connection ping failed. Trying reconnect (attempt $i). Reason: $errCode"
                        );
                    }
                    $this->internalStorage[Co::getCid()] = $connection;
                    $this->statsStorage[$connection]     = $stats;

                    break;
                } catch (Throwable $e) {
                    $errCode = '';
                    if ($connection instanceof PostgreSQL) {
                        $errCode    = (int) $connection->errCode;
                        $connection = null;
                    }
                    $lastException = $e instanceof DBALException
                        ? $e
                        : new ConnectionException($e->getMessage(), (string) $errCode, '', (int) $e->getCode(), $e);
                    $this->sleep($this->retryDelay);  // Sleep s after failure
                }
            }
            if (! $connection instanceof PostgreSQL) {
                $lastException instanceof Throwable
                    ? throw $lastException
                    : throw new ConnectionException('Connection could not be initiated');
            }
        }

        return $this->internalStorage[Co::getCid()];
    }

    public function connectionStats(PostgreSQL $connection) : ?ConnectionStats
    {
        return $this->statsStorage[$connection] ?? null;
    }

    private function onDefer() : void
    {
        $connection = $this->internalStorage[Co::getCid()] ?? null;
        if (! $connection instanceof PostgreSQL) {
            return;
        }
        $stats = $this->connectionStats($connection);
        if ($stats instanceof ConnectionStats) {
            $stats->lastInteraction = time();
        }
        $this->pool->put($connection);
        /** @psalm-suppress MixedArrayOffset */
        unset($this->internalStorage[Co::getCid()]);
        $this->statsStorage->offsetUnset($connection);
    }

    private function sleep(int $seconds) : void
    {
        if (Co::getCid() > 0) {
            Co::sleep($seconds);

            return;
        }
        /** @psalm-suppress ArgumentTypeCoercion */
        sleep($seconds);
    }
}
