<?php

declare(strict_types=1);

namespace Chronhub\Snapshot\Projection;

use Chronhub\Chronicler\Support\Contracts\Aggregate\AggregateRepositoryWithSnapshotting;
use Chronhub\Chronicler\Support\Contracts\Aggregate\AggregateRootWithSnapshotting;
use Chronhub\Foundation\Aggregate\AggregateChanged;
use Chronhub\Foundation\Support\Contracts\Aggregate\AggregateId;
use Chronhub\Foundation\Support\Contracts\Clock\Clock;
use Chronhub\Foundation\Support\Contracts\Message\Header;
use Chronhub\Projector\Support\Contracts\Support\ReadModel;
use Chronhub\Snapshot\Snapshot;
use Chronhub\Snapshot\Store\SnapshotStore;
use Illuminate\Support\Collection;

final class SnapshotReadModel implements ReadModel
{
    private Collection $aggregateCache;

    public function __construct(private AggregateRepositoryWithSnapshotting $aggregateRepository,
                                private SnapshotStore $snapshotStore,
                                private Clock $clock,
                                private array $aggregateTypes)
    {
        $this->aggregateCache = new Collection();
    }

    /**
     * @param string             $operation
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
            $aggregateId = $this->determineAggregateId(
                $event->header(Header::AGGREGATE_ID),
                $event->header(Header::AGGREGATE_ID_TYPE),
            );

            $aggregateIdString = $aggregateId->toString();

            /** @var AggregateRootWithSnapshotting $aggregateRoot */
            $aggregateRoot = $this->aggregateRepository->retrieve($aggregateId);

            if ($aggregateRoot) {
                $snapshot = new Snapshot(
                    $event->header(Header::AGGREGATE_TYPE),
                    $aggregateIdString,
                    $aggregateRoot,
                    $aggregateRoot->version(),
                    $this->clock->fromNow()->dateTime(),
                );

                $this->snapshotStore->save($snapshot);
            }

            $this->aggregateRepository->flushCache();
        });

        $this->aggregateCache = new Collection();
    }

    public function reset(): void
    {
        foreach ($this->aggregateTypes as $aggregateType) {
            $this->snapshotStore->deleteAll($aggregateType);
        }
    }

    public function down(): void
    {
        foreach ($this->aggregateTypes as $aggregateType) {
            $this->snapshotStore->deleteAll($aggregateType);
        }
    }

    public function isInitialized(): bool
    {
        return true;
    }

    public function initialize(): void
    {
    }

    private function determineAggregateId(string|AggregateId $aggregateId, ?string $aggregateIdType): AggregateId
    {
        if ($aggregateId instanceof AggregateId) {
            return $aggregateId;
        }

        /* @var AggregateId $aggregateIdType */
        return $aggregateIdType::fromString($aggregateId);
    }
}
