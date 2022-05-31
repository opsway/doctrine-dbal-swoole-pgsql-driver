<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\DBAL\Swoole\PgSQL;

use Doctrine\DBAL\Driver\Result as ResultInterface;
use Doctrine\DBAL\Driver\Statement as StatementInterface;
use Doctrine\DBAL\ParameterType;
use OpsWay\Doctrine\DBAL\Swoole\PgSQL\Exception\DriverException as SwooleDriverException;

use function is_array;
use function is_bool;
use function is_resource;
use function uniqid;

final class Statement implements StatementInterface
{
    private string $key;
    private array $params = [];

    public function __construct(private ConnectionWrapperInterface $connection, string $sql)
    {
        $this->key = uniqid('stmt_', true);
        if ($this->connection->prepare($this->key, $sql) === false) {
            throw SwooleDriverException::fromConnection($this->connection);
        }
    }

    /**
     * {@inheritdoc}
     *
     * @param int|string $param
     * @param mixed      $value
     * @param int        $type
     */
    public function bindValue($param, $value, $type = ParameterType::STRING) : bool
    {
        $this->params[$param] = $this->escapeValue($value, $type);

        return true;
    }

    /**
     * {@inheritdoc}
     *
     * @param int|string $param
     * @param mixed      $variable
     * @param int        $type
     * @param int|null   $length
     */
    public function bindParam($param, &$variable, $type = ParameterType::STRING, $length = null) : bool
    {
        return $this->bindValue($param, $variable, $type);
    }

    /**
     * @param mixed|null $params
     * @throws SwooleDriverException
     * @psalm-suppress ImplementedReturnTypeMismatch
     */
    public function execute($params = []) : ResultInterface
    {
        $mergedParams = $this->params;
        if (! empty($params)) {
            $params = is_array($params) ? $params : [$params];
            /** @psalm-var mixed|null $param */
            foreach ($params as $key => $param) {
                /** @psalm-suppress MixedAssignment */
                $mergedParams[$key] = $this->escapeValue($param);
            }
        }

        $result = $this->connection->execute($this->key, $mergedParams);
        if (! is_resource($result)) {
            throw SwooleDriverException::fromConnection($this->connection);
        }

        return new Result($this->connection, $result);
    }

    public function errorCode() : int
    {
        return $this->connection->errorCode();
    }

    public function errorInfo() : string
    {
        return $this->connection->error();
    }

    private function escapeValue(mixed $value, int $type = ParameterType::STRING) : ?string
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
