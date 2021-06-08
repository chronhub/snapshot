<?php

declare(strict_types=1);

namespace Chronhub\Snapshot\Projection;

use Chronhub\Chronicler\Exception\StreamNotFound;
use Chronhub\Chronicler\Stream\StreamName;
use Chronhub\Chronicler\Support\Contracts\Aggregate\AggregateRepositoryWithSnapshotting;
use Chronhub\Chronicler\Support\Contracts\Aggregate\AggregateRootWithSnapshotting;
use Chronhub\Chronicler\Support\Contracts\Query\QueryFilter;
use Chronhub\Foundation\Aggregate\AggregateChanged;
use Chronhub\Foundation\Support\Contracts\Aggregate\AggregateId;
use Chronhub\Foundation\Support\Contracts\Aggregate\AggregateRoot;
use Chronhub\Foundation\Support\Contracts\Clock\Clock;
use Chronhub\Foundation\Support\Contracts\Message\Header;
use Chronhub\Projector\Support\Contracts\Support\ReadModel;
use Chronhub\Snapshot\Snapshot;
use Chronhub\Snapshot\Store\SnapshotStore;
use Generator;
use Illuminate\Database\Query\Builder;
use Illuminate\Support\Collection;

final class SnapshotReadModel implements ReadModel
{
    private Collection $aggregateCache;

    public function __construct(private AggregateRepositoryWithSnapshotting $aggregateRepository,
                                private SnapshotStore $snapshotStore,
                                private StreamName $streamName,
                                private Clock $clock,
                                private array $aggregateTypes,
                                private int $persisEveryEvents = 1000)
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
            $version = (int)$event->header(Header::AGGREGATE_VERSION);

            if (1 === $version || 0 === $version % $this->persisEveryEvents) {
                $aggregateId = $this->determineAggregateId($event);
                $from = $version === $this->persisEveryEvents ? 1 : $this->persisEveryEvents - $version;

                $aggregateRoot = $this->reconstituteFromSnapshot(
                    $aggregateId,
                    $event->header(Header::AGGREGATE_TYPE),
                    $from,
                    $version
                );

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

    private function reconstituteFromSnapshot(AggregateId $aggregateId,
                                              string $aggregateType,
                                              int $from,
                                              int $to): ?AggregateRoot
    {
        try{
            $lastSnapshot = $this->snapshotStore->get($aggregateType, $aggregateId->toString());

            if(null === $lastSnapshot){
                return null;
            }

            $aggregateRoot = $lastSnapshot->aggregateRoot();

            $aggregateRoot->reconstituteFromSnapshotEvents($this->fromHistory($aggregateId, $from, $to));

            return $aggregateRoot;
        }catch(StreamNotFound){
            return null;
        }
    }

    private function fromHistory(AggregateId $aggregateId, int $from, int $to): Generator
    {
        $filter = new class($aggregateId, $from, $to) implements QueryFilter {
            public function __construct(private AggregateId $aggregateId, private int $from, private int $to)
            {
            }

            public function filterQuery(): callable
            {
                return function (Builder $query): void {
                    $query
                        ->whereJsonContains('headers->__aggregate_id', $this->aggregateId->toString())
                        ->whereRaw('CAST(headers->>\'__aggregate_version\' AS INT) >= ' . $this->from)
                        ->whereRaw('CAST(headers->>\'__aggregate_version\' AS INT) <= ' . $this->to)
                        ->orderByRaw('CAST(headers->>\'__aggregate_version\' AS INT) ASC');
                };
            }
        };

        return $this->aggregateRepository->chronicler()->retrieveFiltered($this->streamName, $filter);
    }
}
