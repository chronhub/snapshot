<?php

declare(strict_types=1);

return [
    'snapshots' => [
        'store' => [
            'default' => 'pgsql',

            'pgsql' => [
                'table_name' => 'snapshots',
                'mapping_tables' => [],
                'serializer' => \Chronhub\Snapshot\Serializer\PgsqlSnapshotSerializer::class,
                'query_scope' => \Chronhub\Projector\Support\Scope\PgsqlProjectionQueryScope::class,
                //'disabled_transaction' => false,
            ],

            'in_memory' => [
                //'disabled_transaction' => false, // checkMe
                'query_scope' => \Chronhub\Projector\Support\Scope\InMemoryProjectionQueryScope::class,
            ],
        ],
    ],

    'load_migrations_from' => 'pgsql', //nullable
];
