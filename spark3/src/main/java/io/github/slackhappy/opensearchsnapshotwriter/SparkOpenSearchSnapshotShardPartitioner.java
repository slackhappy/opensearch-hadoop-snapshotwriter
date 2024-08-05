package io.github.slackhappy.opensearchsnapshotwriter;

/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2024 John Gallagher
 */

import org.apache.spark.Partitioner;

/**
 * SparkOpenSearchSnapshotShardPartitioner
 * It partitions the data according to the OpenSearch's OperationRouting logic
 * to determine where shards should go based on an _id key.  Assumes that key.toString()
 * is the document's _id.
 *
 */
public class SparkOpenSearchSnapshotShardPartitioner extends Partitioner {
    transient private ShardPartitionUtil partitionUtil;
    private final int numShards;

    public SparkOpenSearchSnapshotShardPartitioner(int numShards) {
        this.numShards = numShards;
    }

    private ShardPartitionUtil partitionUtil() {
        if (partitionUtil == null) {
            partitionUtil = new ShardPartitionUtil(numShards);
        }
        return partitionUtil;
    }

    @Override
    public int numPartitions() {
        return numShards;
    }

    @Override
    public int getPartition(Object key) {
        return partitionUtil().shardId(key.toString());
    }
}
