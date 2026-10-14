<?php

namespace FiberInterop\Swoole;

/**
 * Native async\mysqli driver: the runtime hooks PHP streams, which mysqlnd uses, so a plain
 * mysqli::query() is cooperative and a query is just a task running it. Registered from
 * register.php only when available.
 */
final class MysqliDriver implements \async\mysqli\Driver
{
    public function start(\mysqli $m, string $sql, int $resultMode): object
    {
        return \async\go(static fn() => $m->query($sql, $resultMode));
    }
}
