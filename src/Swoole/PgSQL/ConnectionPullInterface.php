<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\DBAL\Swoole\PgSQL;

interface ConnectionPullInterface
{
    public function get(float $timeout = -1) : ConnectionWrapperInterface|bool|null;

    /** @param ConnectionWrapperInterface|null $connection */
    public function put($connection, bool $updateLastInteraction = true) : void;

    public function removeConnect(?ConnectionWrapperInterface $connection) : void;

    public function close() : void;
}
