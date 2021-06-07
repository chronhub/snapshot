<?php

declare(strict_types=1);

namespace Chronhub\Snapshot\Projection;

use Chronhub\Snapshot\Snapshot;
use Illuminate\Support\Collection;
use Chronhub\Snapshot\Store\SnapshotStore;
use Chronhub\Foundation\Aggregate\AggregateChanged;
use Chronhub\Foundation\Support\Contracts\Clock\Clock;
use Chronhub\Foundation\Support\Contracts\Message\Header;
use Chronhub\Projector\Support\Contracts\Support\ReadModel;
use Chronhub\Foundation\Support\Contracts\Aggregate\AggregateId;
use Chronhub\Chronicler\Support\Contracts\Aggregate\AggregateRootWithSnapshotting;
use Chronhub\Chronicler\Support\Contracts\Aggregate\AggregateRepositoryWithSnapshotting;

final class SnapshotReadModel implements ReadModel
{
    private array $counter = [];
    private Collection $aggregateCache;

    public function __construct(private AggregateRepositoryWithSnapshotting $aggregateRepository,
                                private SnapshotStore $snapshotStore,
                                private Clock $clock,
                                private array $aggregateTypes)
    {
        $this->aggregateCache = new Collection();
    }

    /**
     * @param AggregateChanged[]
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

            $eventVersion = (int) $event->header(Header::AGGREGATE_VERSION);

            /** @var AggregateRootWithSnapshotting $aggregateRoot */
            $aggregateRoot = $this->aggregateRepository->retrieve($aggregateId);

            if ($aggregateRoot) {
                $this->incrementCounter($aggregateIdString);

                if ($this->canBePersisted($aggregateRoot->version(), $aggregateIdString, $eventVersion)) {
                    $snapshot = new Snapshot(
                        $event->header(Header::AGGREGATE_TYPE),
                        $aggregateIdString,
                        $aggregateRoot,
                        $aggregateRoot->version(),
                        $this->clock->fromNow()->dateTime(),
                    );

                    $this->snapshotStore->save($snapshot);
                }

                $this->lastEventVersion = $eventVersion;

                $this->aggregateRepository->flushCache();
            }
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

    private function canBePersisted(int $aggregateVersion, string $aggregateId, int $eventVersion): bool
    {
        return true;
    }

    private function determineAggregateId(string|AggregateId $aggregateId, ?string $aggregateIdType): AggregateId
    {
        if ($aggregateId instanceof AggregateId) {
            return $aggregateId;
        }

        /* @var AggregateId $aggregateIdType */
        return $aggregateIdType::fromString($aggregateId);
    }

    private function incrementCounter(string $aggregateId): void
    {
        isset($this->counter[$aggregateId]) ? $this->counter[$aggregateId]++ : $this->counter[$aggregateId] = 0;
    }
}
