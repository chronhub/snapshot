<?php

declare(strict_types=1);

namespace Chronhub\Snapshot\Serializer;

use function serialize;
use function unserialize;
use function base64_decode;
use function base64_encode;

final class PgsqlSnapshotSerializer implements SnapshotSerializer
{
    public function serialize(mixed $data): string
    {
        $serialized = serialize($data);

        return base64_encode($serialized);
    }

    public function unserialize(string $serialized): mixed
    {
        $serialized = base64_decode($serialized);

        return unserialize($serialized);
    }
}
