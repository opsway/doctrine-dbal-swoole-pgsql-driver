<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\DBAL\Swoole;

final class PostgresqlUtil
{
    public static function isStatementAvailable() : bool
    {
        $version = phpversion('openswoole') ?: phpversion('swoole');
        if ($version === false) {
            throw new \RuntimeException('No OpenSwoole or Swoole extension not found');
        }
        if (version_compare($version, '4.13.0', '<=')) {
            // When using Swoole v5.0 or OpenSwoole v22.0 or higher, then we can use native Postgresql Statement class object
            return true;
        }
        return false;
    }
}
