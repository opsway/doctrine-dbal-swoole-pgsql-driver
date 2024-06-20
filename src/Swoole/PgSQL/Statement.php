<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\DBAL\Swoole\PgSQL;

use Doctrine\DBAL\Driver\Result as ResultInterface;
use Doctrine\DBAL\Driver\Statement as StatementInterface;
use Doctrine\DBAL\ParameterType;
use OpsWay\Doctrine\DBAL\Swoole\PgSQL\Exception\DriverException as SwooleDriverException;
use OpsWay\Doctrine\DBAL\Swoole\PostgresqlUtil;
use Swoole\Coroutine\PostgreSQL;

use function is_bool;
use function uniqid;

final class Statement implements StatementInterface
{
    private readonly object $statement;
    private array $params = [];

    public function __construct(private PostgreSQL $connection, string $sql, private ?ConnectionStats $stats)
    {
        $stmt = false;
        if (PostgresqlUtil::isStatementAvailable()) {
            $stmt = $this->connection->prepare($sql);
        } else {
            $key = uniqid('stmt_', true);
            if ($this->connection->prepare($key, $sql) !== false) {
                $stmt = new class ($this->connection, $key) {
                    public function __construct(private readonly PostgreSQL $connection, private readonly string $key)
                    {
                    }

                    public function execute(array $params) : mixed
                    {
                        return $this->connection->execute($this->key, $params);
                    }
                };
            }
        }
        if ($stmt === false) {
            throw SwooleDriverException::fromConnection($this->connection);
        }
        $this->statement = $stmt;
    }

    /**
     * {@inheritdoc}
     */
    public function bindValue(int|string $param, mixed $value, ParameterType $type = ParameterType::STRING) : void
    {
        $this->params[$param] = $this->escapeValue($value, $type);
    }

    /**
     * @throws SwooleDriverException
     * @psalm-suppress ImplementedReturnTypeMismatch
     */
    public function execute() : ResultInterface
    {
        $result = $this->statement->execute($this->params);
        if ($this->stats instanceof ConnectionStats) {
            $this->stats->counter++;
        }

        return new Result($this->connection, $result, $this->statement);
    }

    private function escapeValue(mixed $value, ParameterType $type = ParameterType::STRING) : ?string
    {
        if ($value !== null && (is_bool($value) || $type === ParameterType::BOOLEAN)) {
            return $value ? 'TRUE' : 'FALSE';
        }

        if ($value === null || $type === ParameterType::NULL) {
            return null;
        }

        return (string) $value;
    }
}
