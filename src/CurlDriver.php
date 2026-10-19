<?php

namespace FiberInterop\Swoole;

/**
 * Native fiber\curl driver: the runtime makes a plain curl_exec() cooperative, so a transfer is
 * just a task running curl_exec(). Registered from register.php only when available.
 */
final class CurlDriver implements \fiber\curl\Driver
{
    public function start(\CurlHandle $h): object
    {
        return \fiber\go(static fn() => \curl_exec($h));
    }
}
