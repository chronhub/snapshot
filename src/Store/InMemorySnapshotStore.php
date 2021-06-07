<?php

declare(strict_types=1);

namespace Chronhub\Snapshot\Store;

use Chronhub\Snapshot\Snapshot;
use Chronhub\Projector\Support\Contracts\ProjectionQueryScope;

final class InMemorySnapshotStore implements SnapshotStore
{
    private array $map = [];

    public function __construct(private ProjectionQueryScope $queryScope)
    {
    }

    public function get(string $aggregateType, string $aggregateId): ?Snapshot
    {
        return $this->map[$aggregateType][$aggregateId] ?? null;
    }

    public function save(Snapshot ...$snapshots): void
    {
        foreach ($snapshots as $snapshot) {
            $this->map[$snapshot->aggregateType()][$snapshot->aggregateId()] = $snapshot;
        }
    }

    public function deleteAll(string $aggregateType): void
    {
        unset($this->map[$aggregateType]);
    }

    public function queryScope(): ProjectionQueryScope
    {
        return $this->queryScope;
    }
}
