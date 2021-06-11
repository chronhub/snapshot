<?php

declare(strict_types=1);

namespace Chronhub\Snapshot\Projection;

use Illuminate\Support\Collection;
use Chronhub\Foundation\Aggregate\AggregateChanged;
use Chronhub\Projector\Support\Contracts\Support\ReadModel;

final class SnapshotReadModel implements ReadModel
{
    private Collection $aggregateCache;

    public function __construct(private SnapshotRepository $repository,
                                private array $aggregateTypes)
    {
        $this->aggregateCache = new Collection();
    }

    /**
     * @param AggregateChanged[] $events
     */
    public function stack(string $operation, ...$events): void
    {
        $event = $events[0];

        $this->aggregateCache->put($event->aggregateId(), $event);
    }

    public function persist(): void
    {
        $this->aggregateCache->each(function (AggregateChanged $event): void {
            $this->repository->store($event);
        });

        $this->aggregateCache = new Collection();
    }

    public function reset(): void
    {
        foreach ($this->aggregateTypes as $aggregateType) {
            $this->repository->deleteAll($aggregateType);
        }
    }

    public function down(): void
    {
        foreach ($this->aggregateTypes as $aggregateType) {
            $this->repository->deleteAll($aggregateType);
        }
    }

    public function isInitialized(): bool
    {
        return true;
    }

    public function initialize(): void
    {
    }
}
