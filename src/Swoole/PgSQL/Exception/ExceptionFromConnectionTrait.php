<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\DBAL\Swoole\PgSQL\Exception;

use ArrayAccess;
use OpsWay\Doctrine\DBAL\Swoole\PgSQL\ConnectionWrapperInterface;

/** @psalm-immutable */
trait ExceptionFromConnectionTrait
{
    public static function fromConnection(ConnectionWrapperInterface $connection) : self
    {
        /** @var ArrayAccess $resultDiag */
        $resultDiag = $connection->resultDiag() ?? [];
        $sqlstate   = (string) ($resultDiag['sqlstate'] ?? '');

        return new self(
            $connection->error(),
            (string) $connection->errorCode(),
            $sqlstate,
            $connection->errorCode(),
        );
    }
}
