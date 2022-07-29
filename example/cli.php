#!/usr/bin/env php
<?php

declare(strict_types=1);

require_once 'vendor/autoload.php';

$connectionParams = [
    'dbname' => 'mydb',
    'user' => 'user',
    'password' => 'secret',
    'host' => 'dbhost',
    'driverClass' => \OpsWay\Doctrine\DBAL\Swoole\PgSQL\Driver::class,
    'poolSize' => 5, // MAX count connections in one pool
    'tickFrequency' => 60000, // when need check possibilities downscale (close) opened connection to DB in pools
    'connectionTtl' => 60, // when connection not used this time(seconds) - it will be close (free)
    'usedTimes' => 100, // 1 connection (in pool) will be re-used maximum N queries
    'connectionDelay' => 2, // time(seconds) for waiting response from pool
    'useConnectionPool' => true, // if false, will create new connect instead of using pool
    'retry' => [
        'maxAttempts' => 2, // if connection in pool was timeout (before use) then try re-connect
        'delay' => 1000, // delay to try fetch from pool again(milliseconds) if no connect available
    ]
];
$pool = (new \OpsWay\Doctrine\DBAL\Swoole\PgSQL\ConnectionPoolFactory())($connectionParams);
$configuration = new \Doctrine\DBAL\Configuration();
$configuration->setMiddlewares(
    [new \OpsWay\Doctrine\DBAL\Swoole\PgSQL\DriverMiddleware($pool)]
);
$connFactory = static fn() => \Doctrine\DBAL\DriverManager::getConnection($connectionParams, $configuration);

Co\run(function () use ($connFactory) {
    for ($i = 1; $i <= 5; $i++) { // get 5 connection and make 5 async calls (2 SQL queries in each connection)
        go(static function () use ($connFactory, $i) {
            $conn = $connFactory();
            $v = $conn->fetchOne('SELECT version()');
            echo $v . PHP_EOL;
            $conn->fetchOne('SELECT pg_sleep(1)');
            echo 'routine #: ' . $i . PHP_EOL;
            defer(static fn() => $conn->close());
        });
    }
    // If repeat this again 50 times then each 5 connection will have same connection to DB by 100 queries count
    // 500 SQL for 50sec (instead of 250sec)
});

