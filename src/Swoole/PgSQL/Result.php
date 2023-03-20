<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\DBAL\Swoole\PgSQL;

use Doctrine\DBAL\Driver\Result as ResultInterface;
use OpsWay\Doctrine\DBAL\Swoole\PgSQL\Exception\DriverException as SwooleDriverException;
use OpsWay\Doctrine\DBAL\Swoole\PostgresqlUtil;
use Swoole\Coroutine\PostgreSQL;

use function count;
use function is_array;
use function is_resource;

use const OPENSWOOLE_PGSQL_NUM;

class Result implements ResultInterface
{
    /** @param resource|null $result */
    public function __construct(private PostgreSQL $connection, private $result, private ?object $statement)
    {
        if (PostgresqlUtil::isStatementAvailable()) {
            if ($result === false) {
                throw SwooleDriverException::fromConnection($this->connection);
            }
        } else {
            if (!is_resource($result)) {
                throw SwooleDriverException::fromConnection($this->connection);
            }
            $this->statement = null;
        }
    }

    /** {@inheritdoc} */
    public function fetchNumeric() : array|bool
    {
        if ($this->result === null) {
            throw new SwooleDriverException('No result set available');
        }
        /**
         * @psalm-var list<mixed>|false $result
         * @psalm-suppress UndefinedConstant
         */
        $result = ($this->statement) ? $this->statement->fetchArray(null, \OpenSwoole\Coroutine\PostgreSQL::PGSQL_NUM)
            : $this->connection->fetchArray($this->result, null, OPENSWOOLE_PGSQL_NUM);

        return $result;
    }

    /** {@inheritdoc} */
    public function fetchAssociative() : array|bool
    {
        if ($this->result === null) {
            throw new SwooleDriverException('No result set available');
        }
        /** @psalm-var array<string,mixed>|false $result */
        $result = ($this->statement) ? $this->statement->fetchAssoc(null)
            : $this->connection->fetchAssoc($this->result, null);
        if (is_array($result) && count($result) === 0) {
            $result = false;
        }

        return $result;
    }

    /** {@inheritdoc} */
    public function fetchOne() : mixed
    {
        $row = $this->fetchNumeric();

        return $row === false ? false : $row[0];
    }

    /** {@inheritdoc} */
    public function fetchAllNumeric() : array
    {
        $rows = [];
        while (($row = $this->fetchNumeric()) !== false) {
            $rows[] = $row;
        }

        return $rows;
    }

    /** {@inheritdoc} */
    public function fetchAllAssociative() : array
    {
        $rows = [];
        while (($row = $this->fetchAssociative()) !== false) {
            $rows[] = $row;
        }

        return $rows;
    }

    /**
     * {@inheritdoc}
     *
     * @psalm-suppress MixedAssignment
     */
    public function fetchFirstColumn() : array
    {
        $rows = [];
        while (($row = $this->fetchOne()) !== false) {
            $rows[] = $row;
        }

        return $rows;
    }

    /** {@inheritdoc} */
    public function rowCount() : int
    {
        if ($this->result === null) {
            throw new SwooleDriverException('No result set available');
        }
        return (int) (($this->statement) ? $this->statement->affectedRows() : $this->connection->affectedRows($this->result));
    }

    /** {@inheritdoc} */
    public function columnCount() : int
    {
        if ($this->result === null) {
            throw new SwooleDriverException('No result set available');
        }
        return (int) (($this->statement) ? $this->statement->fieldCount() : $this->connection->fieldCount($this->result));
    }

    /** {@inheritdoc} */
    public function free() : void
    {
        $this->result = null;
        $this->statement = null;
    }
}
