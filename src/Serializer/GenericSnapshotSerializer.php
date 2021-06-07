<?php

declare(strict_types=1);

namespace Chronhub\Snapshot\Serializer;

use function serialize;
use function unserialize;

final class GenericSnapshotSerializer implements SnapshotSerializer
{
    public function serialize(mixed $data): string
    {
        return serialize($data);
    }

    public function unserialize(string $serialized): mixed
    {
        return unserialize($serialized);
    }
}
