package io.github.slackhappy.opensearchsnapshotwriter;

/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2024 John Gallagher
 */

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.common.settings.Settings;


/**
 * ShardPartitionUtil is used by OpenSearchSnapshotOutputFormat.
 * It helps identify the partition OpenSearch would use for routing
 * a document given a number of shards
 */

public class ShardPartitionUtil {
    private final int numShards;
    transient private IndexMetadata indexMetadata;

    public ShardPartitionUtil(int numShards) {
        this.numShards = numShards;
    }

    private IndexMetadata indexMetadata() {
        if (indexMetadata != null) {
            return indexMetadata;
        }
        indexMetadata = IndexMetadata.builder("foo")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(numShards)
            .numberOfReplicas(0)
            .build();
        return indexMetadata;
    }

    public Integer shardId(String id) {
        return OperationRouting.generateShardId(indexMetadata(), id, null);
    }
}



