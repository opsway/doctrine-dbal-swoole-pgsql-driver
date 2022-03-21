<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\DBAL\Swoole\PgSQL;

use Doctrine\DBAL\Driver\Connection as ConnectionInterface;
use Doctrine\DBAL\ParameterType;
use Exception;
use OpsWay\Doctrine\DBAL\SQLParserUtils;

use function strlen;
use function substr;

final class Connection implements ConnectionInterface
{
    public function __construct(private ConnectionWrapperInterface $connection)
    {
    }

    /**
     * {@inheritdoc}
     *
     * @throws Exception
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

        return new Statement($this->connection, $sql);
    }

    /**
     * {@inheritdoc}
     */
    public function query(string $sql) : Result
    {
        /** @var resource $resource */
        $resource = $this->connection->query($sql);

        return new Result($this->connection, $resource);
    }

    /**
     * {@inheritdoc}
     *
     * @param mixed $value
     * @param int   $type
     */
    public function quote($value, $type = ParameterType::STRING) : string
    {
        return "'" . (string) $this->connection->escape($value) . "'";
    }

    /**
     * {@inheritdoc}
     */
    public function exec(string $sql) : int
    {
        $query = $this->connection->query($sql);
        if ($query !== false) {
            return $this->connection->affectedRows($query);
        }

        return 0;
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
        $this->connection->query('START TRANSACTION');

        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function commit() : bool
    {
        $this->connection->query('COMMIT');

        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function rollBack() : bool
    {
        $this->connection->query('ROLLBACK');

        return true;
    }

    public function errorCode() : int
    {
        return $this->connection->errorCode();
    }

    public function errorInfo() : string
    {
        return $this->connection->error();
    }
}
