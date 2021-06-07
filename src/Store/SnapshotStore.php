<?php

declare(strict_types=1);

namespace Chronhub\Snapshot\Store;

use Chronhub\Snapshot\Snapshot;
use Chronhub\Projector\Support\Contracts\ProjectionQueryScope;

interface SnapshotStore
{
    public function get(string $aggregateType, string $aggregateId): ?Snapshot;

    public function save(Snapshot ...$snapshots): void;

    public function deleteAll(string $aggregateType): void;

    public function queryScope(): ProjectionQueryScope;
}
