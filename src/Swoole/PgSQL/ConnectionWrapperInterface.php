<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\DBAL\Swoole\PgSQL;

use ArrayAccess;

interface ConnectionWrapperInterface
{
    public function updateLastInteraction() : void;

    public function isUptoDate() : bool;

    public function id() : string;

    public function isReusable() : bool;

    /** @return  resource|false */
    public function query(string $sql);

    /** @param resource $queryResult */
    public function affectedRows($queryResult) : int;

    /** @param resource $queryResult */
    public function fetchAssoc($queryResult) : array|bool;

    /** @param resource $queryResult */
    public function fetchArray($queryResult, ?int $row = null, mixed $resultType = null) : array|bool;

    public function prepare(string $key, string $sql) : Result|bool;

    /** @psalm-return resource|null */
    public function execute(string $key, array $params);

    /** @return mixed */
    public function escape(mixed $value);

    /** @param resource|null $result */
    public function fieldCount($result) : int;

    public function error() : string;

    public function errorCode() : int;

    /** @psalm-return void */
    public function __destruct();

    /** @return  ArrayAccess|array|null */
    public function resultDiag();
}
