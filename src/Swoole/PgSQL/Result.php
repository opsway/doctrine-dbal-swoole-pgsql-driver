<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\DBAL\Swoole\PgSQL;

use Doctrine\DBAL\Driver\Result as ResultInterface;
use OpsWay\Doctrine\DBAL\Swoole\PgSQL\Exception\DriverException as SwooleDriverException;

use function is_resource;

use const OPENSWOOLE_PGSQL_NUM;

class Result implements ResultInterface
{
    /** @param resource|null $result */
    public function __construct(private ConnectionWrapperInterface $connection, private $result)
    {
    }

    /** {@inheritdoc} */
    public function fetchNumeric() : array|bool
    {
        if (! is_resource($this->result)) {
            throw SwooleDriverException::fromConnection($this->connection);
        }
        /**
         * @psalm-var list<mixed>|false $result
         * @psalm-suppress UndefinedConstant
         */
        $result = $this->connection->fetchArray($this->result, resultType: OPENSWOOLE_PGSQL_NUM);

        return $result;
    }

    /** {@inheritdoc} */
    public function fetchAssociative() : array|bool
    {
        if (! is_resource($this->result)) {
            throw SwooleDriverException::fromConnection($this->connection);
        }
        /** @psalm-var array<string,mixed>|false $result */
        $result = $this->connection->fetchAssoc($this->result);

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
        if (! is_resource($this->result)) {
            throw SwooleDriverException::fromConnection($this->connection);
        }

        return $this->connection->affectedRows($this->result);
    }

    /** {@inheritdoc} */
    public function columnCount() : int
    {
        if (! is_resource($this->result)) {
            throw SwooleDriverException::fromConnection($this->connection);
        }

        return $this->connection->fieldCount($this->result);
    }

    /** {@inheritdoc} */
    public function free() : void
    {
        $this->result = null;
    }
}
