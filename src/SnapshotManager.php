<?php

declare(strict_types=1);

namespace Chronhub\Snapshot;

use Illuminate\Support\Arr;
use Illuminate\Support\Str;
use Chronhub\Snapshot\Store\SnapshotStore;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Database\ConnectionInterface;
use Illuminate\Contracts\Foundation\Application;
use Chronhub\Snapshot\Store\InMemorySnapshotStore;
use Chronhub\Snapshot\Store\ConnectionSnapshotStore;
use Chronhub\Foundation\Support\Contracts\Clock\Clock;
use Chronhub\Snapshot\Exception\InvalidArgumentException;
use function method_exists;

class SnapshotManager
{
    protected array $snapshotStore = [];
    protected array $customSnapshotStore = [];
    protected array $config;

    public function __construct(protected Application $app)
    {
        $this->config = $app->get(Repository::class)->get('snapshot');
    }

    public function create(string $driver): SnapshotStore
    {
        if ($snapshotStore = $this->snapshotStore[$driver] ?? null) {
            return $snapshotStore;
        }

        return $this->snapshotStore[$driver] = $this->resolveSnapshotStore($driver);
    }

    public function extend(string $driver, callable $snapshotStore): void
    {
        $this->customSnapshotStore[$driver] = $snapshotStore;
    }

    protected function resolveSnapshotStore(string $driver): SnapshotStore
    {
        if ($customSnapshotStore = $this->customSnapshotStore[$driver] ?? null) {
            return $customSnapshotStore($this->app, $this->config);
        }

        $method = 'create' . Str::studly($driver . 'SnapshotStore');

        $config = $this->fromSnapshotConfig("snapshots.store.$driver");

        if (method_exists($this, $method)) {
            return $this->$method($config);
        }

        throw new InvalidArgumentException("Invalid driver $driver for snapshot store");
    }

    protected function createConnectionSnapshotStore(ConnectionInterface $connection, array $store): SnapshotStore
    {
        return new ConnectionSnapshotStore(
            $connection,
            $this->app->make($store['serializer']),
            $this->app->make($store['query_scope']),
            $this->app->get(Clock::class),
            $store['table_name'],
            $store['mapping_tables'] ?? [],
        );
    }

    protected function createPgsqlSnapshotStore(array $store): SnapshotStore
    {
        return $this->createConnectionSnapshotStore(
            $this->app['db']->connection('pgsql'), $store
        );
    }

    protected function createInMemorySnapshotStore(array $store): SnapshotStore
    {
        return new InMemorySnapshotStore(
            $this->app->make($store['query_scope'])
        );
    }

    protected function fromSnapshotConfig(string $key, $default = null)
    {
        return Arr::get($this->config, $key, $default);
    }
}
