<?php

declare(strict_types=1);

namespace Chronhub\Snapshot\Aggregate;

use Chronhub\Snapshot\Snapshot;
use Chronhub\Snapshot\Store\SnapshotStore;
use Chronhub\Chronicler\Exception\StreamNotFound;
use Chronhub\Chronicler\Support\Contracts\Chronicler;
use Chronhub\Chronicler\Aggregate\AggregateEventReleaser;
use Chronhub\Chronicler\Support\Contracts\StreamProducer;
use Chronhub\Chronicler\Aggregate\HasReconstituteAggregate;
use Chronhub\Chronicler\Support\Contracts\Query\QueryFilter;
use Chronhub\Chronicler\Support\Contracts\ReadOnlyChronicler;
use Chronhub\Foundation\Support\Contracts\Aggregate\AggregateId;
use Chronhub\Chronicler\Support\Contracts\Aggregate\AggregateType;
use Chronhub\Foundation\Support\Contracts\Aggregate\AggregateRoot;
use Chronhub\Chronicler\Support\Contracts\Aggregate\AggregateCache;
use Chronhub\Chronicler\Support\Contracts\Aggregate\AggregateRootWithSnapshotting;
use Chronhub\Chronicler\Support\Contracts\Aggregate\AggregateRepositoryWithSnapshotting;
use function reset;

final class AggregateSnapshotRepository implements AggregateRepositoryWithSnapshotting
{
    use HasReconstituteAggregate;

    public function __construct(protected AggregateType $aggregateType,
                                protected Chronicler $chronicler,
                                protected StreamProducer $streamProducer,
                                protected AggregateCache $aggregateCache,
                                protected AggregateEventReleaser $eventsReleaser,
                                protected SnapshotStore $snapshotStore)
    {
    }

    public function retrieve(AggregateId $aggregateId): ?AggregateRoot
    {
        if ($this->aggregateCache->has($aggregateId)) {
            return $this->aggregateCache->get($aggregateId);
        }

        if ($aggregateRoot = $this->retrieveFromSnapshotStore($aggregateId)) {
            $this->aggregateCache->put($aggregateRoot);

            return $aggregateRoot;
        }

        $aggregateRoot = $this->reconstituteAggregateRoot($aggregateId);

        if ($aggregateRoot) {
            $this->aggregateCache->put($aggregateRoot);
        }

        return $aggregateRoot;
    }

    public function persist(AggregateRoot $aggregateRoot): void
    {
        $this->aggregateType->assertAggregateRootIsSupported($aggregateRoot::class);

        $events = $this->eventsReleaser->releaseEvents($aggregateRoot);

        if ( ! $firstEvent = reset($events)) {
            return;
        }

        $stream = $this->streamProducer->produceStream($aggregateRoot->aggregateId(), $events);

        $this->streamProducer->isFirstCommit($firstEvent)
            ? $this->chronicler->persistFirstCommit($stream)
            : $this->chronicler->persist($stream);

        $this->aggregateCache->forget($aggregateRoot->aggregateId());
    }

    public function chronicler(): ReadOnlyChronicler
    {
        return $this->chronicler;
    }

    public function flushCache(): void
    {
        $this->aggregateCache->flush();
    }

    public function retrieveFromSnapshotStore(AggregateId $aggregateId): ?AggregateRootWithSnapshotting
    {
        $snapshot = $this->snapshotStore->get(
            $this->aggregateType->aggregateRootClassName(),
            $aggregateId->toString()
        );

        if ( ! $snapshot) {
            return null;
        }

        $aggregateRoot = $snapshot->aggregateRoot();

        try {
            $streamEvents = $this->fromHistory(
                $aggregateId,
                $this->retrieveAllEventsFromSnapshotLastVersion($aggregateId, $snapshot)
            );

            return $aggregateRoot->reconstituteFromSnapshotEvents($streamEvents);
        } catch (StreamNotFound) {
            return $aggregateRoot; // null ?
        }
    }

    private function retrieveAllEventsFromSnapshotLastVersion(AggregateId $aggregateId,
                                                              Snapshot $snapshot): QueryFilter
    {
        $queryScope = $this->snapshotStore->queryScope();

        if ($this->streamProducer->isOneStreamPerAggregate()) {
            $queryFilter = $queryScope->fromIncludedPosition();

            $queryFilter->setCurrentPosition($snapshot->lastVersion() + 1);

            return $queryFilter;
        }

        return $this->snapshotStore->queryScope()->matchAggregateGreaterThanVersion(
            $aggregateId->toString(),
            $this->aggregateType->aggregateRootClassName(),
            $snapshot->lastVersion()
        );
    }
}
