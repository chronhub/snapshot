<?php

declare(strict_types=1);

namespace Chronhub\Snapshot\Aggregate;

use Generator;
use Chronhub\Snapshot\Exception\RuntimeException;
use Chronhub\Chronicler\Support\Contracts\Aggregate\AggregateRootWithSnapshotting;

trait HasReconstituteSnapshottingAggregate
{
    public function reconstituteFromSnapshotEvents(Generator $events): ?AggregateRootWithSnapshotting
    {
        /** @var AggregateRootWithSnapshotting $this */
        if ($this->version() < 1) {
            throw new RuntimeException('Can not reconstitute aggregate with a zero version');
        }

        foreach ($events as $event) {
            $this->apply($event);
        }

        return $this;
    }
}
