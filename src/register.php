<?php

declare(strict_types=1);

if (\extension_loaded('swoole') || \extension_loaded('openswoole')) {
    \async\register(static fn() => new \FiberInterop\Swoole\Adapter(), 'fiber-interop/swoole');
}

// Native async\curl driver only where curl_exec() is hooked as real libcurl: SWOOLE_HOOK_NATIVE_CURL
// (builds with --enable-swoole-curl; part of the SWOOLE_HOOK_ALL the adapter sets). The legacy
// SWOOLE_HOOK_CURL swaps curl_init() for Swoole\Curl\Handler and rejects a CurlHandle made outside a
// coroutine, so without native curl the generic MultiDriver is used.
if (\defined('SWOOLE_HOOK_NATIVE_CURL') && (\SWOOLE_HOOK_ALL & \SWOOLE_HOOK_NATIVE_CURL) && \extension_loaded('curl') && \interface_exists(\async\curl\Driver::class)) {
    \async\curl\Registry::$candidates['fiber-interop/swoole'] = static fn() => new \FiberInterop\Swoole\CurlDriver();
}
