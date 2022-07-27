<?php

declare(strict_types=1);

use OpsWay\Doctrine\DBAL\Swoole\PgSQL\Scaler;
use Swoole\Http\Server;
use Swoole\Http\Request;
use Swoole\Http\Response;

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
    'retry' => [
        'maxAttempts' => 2, // if connection in pool was timeout (before use) then try re-connect
        'delay' => 1, // after this time
    ]
];
$pool = (new \OpsWay\Doctrine\DBAL\Swoole\PgSQL\ConnectionPoolFactory())($connectionParams);
$configuration = new \Doctrine\DBAL\Configuration();
$configuration->setMiddlewares(
    [new \OpsWay\Doctrine\DBAL\Swoole\PgSQL\DriverMiddleware($pool)]
);
$connFactory = static fn() => \Doctrine\DBAL\DriverManager::getConnection($connectionParams, $configuration);
$scaler      = new Scaler($pool, $connectionParams['tickFrequency']); // will try to free idle connect on connectionTtl overdue

$server = new Swoole\HTTP\Server("0.0.0.0", 9501);

$server->on("Start", function(Server $server)
{
    echo "Swoole http server is started at http://0.0.0.0:9501\n";
});

$server->on("Request", function(Request $request, Response $response) use ($connFactory)
{
    go(static function () use ($connFactory, $response) {
        $conn = $connFactory();
        $conn->fetchOne('SELECT version()');
        $conn->fetchOne('SELECT pg_sleep(2)');
        defer(static fn() => $conn->close());
        $response->header("Content-Type", "text/plain");
        $response->end('End');
    });
});

$server->on('workerstart', function() use ($scaler)
{
    $scaler->run();
});

$server->on('workerstop', function() use ($pool, $scaler)
{
    $pool->close();
    $scaler->close();
});

$server->start();
