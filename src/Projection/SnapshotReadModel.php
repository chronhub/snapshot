<?php

declare(strict_types=1);

namespace Chronhub\Snapshot\Projection;

use Chronhub\Snapshot\Snapshot;
use Illuminate\Support\Collection;
use Chronhub\Snapshot\Store\SnapshotStore;
use Chronhub\Foundation\Aggregate\AggregateChanged;
use Chronhub\Foundation\Support\Contracts\Clock\Clock;
use Chronhub\Foundation\Support\Contracts\Message\Header;
use Chronhub\Snapshot\Exception\InvalidArgumentException;
use Chronhub\Projector\Support\Contracts\Support\ReadModel;
use Chronhub\Foundation\Support\Contracts\Aggregate\AggregateId;
use Chronhub\Chronicler\Support\Contracts\Aggregate\AggregateRootWithSnapshotting;

final class SnapshotReadModel implements ReadModel
{
    private Collection $aggregateCache;

    public function __construct(private SnapshotStore $snapshotStore,
                                private ReconstituteAggregateForSnapshot $reconstitute,
                                private Clock $clock,
                                private array $aggregateTypes,
                                private int $persisEveryEvents = 1000)
    {
        if ($persisEveryEvents <= 1) {
            throw new InvalidArgumentException('Persist every x events must be greater than one');
        }

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
            $version = (int) $event->header(Header::AGGREGATE_VERSION);

            if (1 === $version || 0 === $version % $this->persisEveryEvents) {
                $aggregateId = $this->determineAggregateId($event);

                $aggregateRoot = $this->reconstituteAggregateRoot($event, $aggregateId, $version);

                if ($aggregateRoot instanceof AggregateRootWithSnapshotting) {
                    $snapshot = new Snapshot(
                        $event->header(Header::AGGREGATE_TYPE),
                        $aggregateId->toString(),
                        $aggregateRoot,
                        $aggregateRoot->version(),
                        $this->clock->fromNow()->dateTime(),
                    );

                    $this->snapshotStore->save($snapshot);
                }
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

    private function reconstituteAggregateRoot(AggregateChanged $event,
                                               AggregateId $aggregateId,
                                               int $version): ?AggregateRootWithSnapshotting
    {
        $aggregateType = $event->header(Header::AGGREGATE_TYPE);

        if (1 !== $version) {
            $lastSnapshot = $this->snapshotStore->get($aggregateType, $aggregateId->toString());

            if (null === $lastSnapshot) {
                return null;
            }

            return $this->reconstitute->reconstituteFromSnapshot($lastSnapshot, $aggregateId, $version);
        }

        return $this->reconstitute->reconstituteFromFirstVersion($aggregateId, $aggregateType, $event);
    }

    private function determineAggregateId(AggregateChanged $event): AggregateId
    {
        $aggregateId = $event->header(Header::AGGREGATE_ID);

        if ($aggregateId instanceof AggregateId) {
            return $aggregateId;
        }

        $aggregateIdType = $event->header(Header::AGGREGATE_ID_TYPE);

        /* @var AggregateId $aggregateIdType */
        return $aggregateIdType::fromString($aggregateId);
    }
}
