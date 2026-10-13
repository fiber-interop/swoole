<?php

namespace FiberInterop\Swoole;

/**
 * Native async\curl driver: the runtime makes a plain curl_exec() cooperative, so a transfer is
 * just a task running curl_exec(). Registered from register.php only when available.
 */
final class CurlDriver implements \async\curl\Driver
{
    public function start(\CurlHandle $h): object
    {
        return \async\go(static fn() => \curl_exec($h));
    }
}
