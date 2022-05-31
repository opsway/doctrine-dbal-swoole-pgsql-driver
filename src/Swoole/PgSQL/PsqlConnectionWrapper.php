<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\DBAL\Swoole\PgSQL;

use ArrayAccess;
use Swoole\Coroutine\PostgreSQL;

use function count;
use function is_array;
use function is_resource;
use function mt_rand;
use function time;
use function uniqid;

/** @psalm-suppress all */
class PsqlConnectionWrapper implements ConnectionWrapperInterface
{
    private int $lastInteraction;
    private string $id;
    public int $usedTimes = 1;

    /** @psalm-suppress all */
    public function __construct(private ?PostgreSQL $connection, private int $ttl, private int $maxUsageTimes)
    {
        $this->lastInteraction = time();
        $this->id              = uniqid((string) mt_rand(), true);
    }

    public function __destruct()
    {
        unset($this->connection);
        $this->connection = null;
    }

    public function updateLastInteraction() : void
    {
        $this->lastInteraction = time();
    }

    public function isUptoDate() : bool
    {
        return $this->lastInteraction + $this->ttl > time();
    }

    public function id() : string
    {
        return $this->id;
    }

    public function times() : int
    {
        return $this->usedTimes;
    }

    public function isReusable() : bool
    {
        return $this->usedTimes < $this->maxUsageTimes;
    }

    /** @return resource|false */
    public function query(string $sql)
    {
        $this->usedTimes++;
        $result = $this->connection->query($sql);

        if (! is_resource($result)) {
            $result = false;
        }

        return $result;
    }

    /** @param resource $queryResult */
    public function affectedRows($queryResult) : int
    {
        return (int) $this->connection->affectedRows($queryResult);
    }

    /** @param resource $queryResult */
    public function fetchAssoc($queryResult) : array|bool
    {
        $result = $this->connection->fetchAssoc($queryResult);

        if (is_array($result) && count($result) === 0) {
            $result = false;
        }

        return $result;
    }

    /** @param resource $queryResult*/
    public function fetchArray($queryResult, int|null $row = null, mixed $resultType = null) : array|bool
    {
        return match (true) {
            $row !== null && $resultType !== null => $this->connection->fetchArray($queryResult, $row, $resultType),
            $row !== null                         => $this->connection->fetchArray($queryResult, $row),
            $resultType !== null                  => $this->connection->fetchArray($queryResult, null, $resultType),
            default                               => $this->connection->fetchArray($queryResult)
        };
    }

    public function prepare(string $key, string $sql) : Result|bool
    {
        return $this->connection->prepare($key, $sql);
    }

    /** @return  resource|false */
    public function execute(string $key, array $params)
    {
        $this->usedTimes++;

        return $this->connection->execute($key, $params);
    }

    /** @return mixed */
    public function escape(mixed $value)
    {
        return $this->connection->escape($value);
    }

    /** @param  resource|null $result*/
    public function fieldCount($result) : int
    {
        return (int) $this->connection->fieldCount($result);
    }

    public function error() : string
    {
        return (string) $this->connection->error;
    }

    public function errorCode() : int
    {
        return (int) $this->connection->errCode;
    }

    /** @return ArrayAccess|array|null */
    public function resultDiag()
    {
        return $this->connection->resultDiag;
    }
}
