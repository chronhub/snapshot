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
use Chronhub\Foundation\Support\Contracts\Aggregate\AggregateId;
use Chronhub\Chronicler\Aggregate\InteractWithAggregateRepository;
use Chronhub\Chronicler\Support\Contracts\Aggregate\AggregateType;
use Chronhub\Foundation\Support\Contracts\Aggregate\AggregateRoot;
use Chronhub\Chronicler\Support\Contracts\Aggregate\AggregateCache;
use Chronhub\Chronicler\Support\Contracts\Aggregate\AggregateRootWithSnapshotting;
use Chronhub\Chronicler\Support\Contracts\Aggregate\AggregateRepositoryWithSnapshotting;

final class AggregateSnapshotRepository implements AggregateRepositoryWithSnapshotting
{
    use HasReconstituteAggregate;
    use InteractWithAggregateRepository { retrieve as retrieveFromChronicler; }

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

        return $this->retrieveFromChronicler($aggregateId);
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
            // no more events have been found
            // we can safely return the aggregate
            // only if we persist aggregate on every event
            return $aggregateRoot;
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
            $snapshot->aggregateType(),
            $snapshot->lastVersion()
        );
    }
}
