<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\DBAL\Swoole\PgSQL;

use Assert\Assertion;
use BadMethodCallException;
use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Types\Type;

use function in_array;
use function sprintf;
use function uniqid;

/**
 * @see Based on https://habr.com/ru/company/lamoda/blog/455571/
 */
class Cursor
{
    public const DIRECTION_NEXT         = 'NEXT';
    public const DIRECTION_PRIOR        = 'PRIOR';
    public const DIRECTION_FIRST        = 'FIRST';
    public const DIRECTION_LAST         = 'LAST';
    public const DIRECTION_ABSOLUTE     = 'ABSOLUTE'; // with count
    public const DIRECTION_RELATIVE     = 'RELATIVE'; // with count
    public const DIRECTION_FORWARD      = 'FORWARD'; // with count
    public const DIRECTION_FORWARD_ALL  = 'FORWARD ALL';
    public const DIRECTION_BACKWARD     = 'BACKWARD'; // with count
    public const DIRECTION_BACKWARD_ALL = 'BACKWARD ALL';

    private string $cursorName;
    private bool $isOpen = false;

    /**
     * @psalm-param array<array-key, string|Type|ParameterType|ArrayParameterType> $paramsTypes
     */
    public function __construct(
        private Connection $connection,
        private string $sql,
        private array $params = [],
        private array $paramsTypes = [],
        private bool $withHold = false,
    ) {
        $this->cursorName = $this->connection->quoteIdentifier(uniqid('cursor_'));
    }

    public function getQuery(int $count = 1, string $direction = self::DIRECTION_FORWARD) : string
    {
        if (! $this->isOpen) {
            $this->openCursor();
        }

        if (
            in_array(
                $direction,
                [self::DIRECTION_FORWARD, self::DIRECTION_BACKWARD, self::DIRECTION_ABSOLUTE, self::DIRECTION_RELATIVE],
                true
            )
        ) {
            return sprintf('FETCH %s %d FROM %s', $direction, $count, $this->cursorName);
        }

        return sprintf('FETCH %s FROM %s', $direction, $this->cursorName);
    }

    public function getFetchQuery(int $count = 1, string $direction = self::DIRECTION_FORWARD) : array
    {
        return $this->connection->fetchAllAssociative($this->getQuery($count, $direction));
    }

    private function openCursor() : void
    {
        if (! $this->withHold && $this->connection->getTransactionNestingLevel() === 0) {
            throw new BadMethodCallException('Cursor must be used inside a transaction');
        }

        $sql = sprintf(
            'DECLARE %s CURSOR %sFOR (%s)',
            $this->cursorName,
            $this->withHold ? 'WITH HOLD ' : '',
            $this->sql,
        );
        $this->connection->executeQuery($sql, $this->params, $this->paramsTypes);
        $this->isOpen = true;
    }

    public function close() : void
    {
        if (! $this->isOpen) {
            return;
        }

        $this->connection->executeStatement('CLOSE ' . $this->cursorName);
        $this->isOpen = false;
    }
}
