<?php

return [
    'driver' => 'nsq',
    'options' => [
        'host' => env('NSQ_HOST', '127.0.0.1'),
        'port' => env('NSQ_PORT', 4150),
        'authSecret' => env('NSQ_AUTH_SECRET')
    ],
    'queue' => [
        'topic' => env('NSQ_TOPIC', 'laravel-queue'),
        'channel' => env('NSQ_CHANNEL', 'default'),
    ],
];
