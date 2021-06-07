<?php

declare(strict_types=1);

namespace Chronhub\Snapshot\Serializer;

interface SnapshotSerializer
{
    public function serialize(mixed $data): string;

    public function unserialize(string $serialized): mixed;
}
