<?php
// Shared by the probes: autoload, a stub Scope if the api ships none, and this adapter.
require __DIR__ . '/../vendor/autoload.php';
if (!class_exists('Scope')) {
    require_once __DIR__ . '/../vendor/fiber-interop/api/tests/Support/ScopeStub.php';
}
