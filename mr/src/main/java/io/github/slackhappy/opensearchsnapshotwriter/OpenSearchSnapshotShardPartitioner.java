package io.github.slackhappy.opensearchsnapshotwriter;

/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2024 John Gallagher
 */

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * OpenSearchSnapshotShardPartitioner is part of OpenSearchSnapshotOutputFormat.
 * It partitions the data according to the OpenSearch's OperationRouting logic
 * @param <T> value type (unused)
 */
public class OpenSearchSnapshotShardPartitioner<T> extends Partitioner<Text, T> implements Configurable {
    private Configuration conf;
    private Integer expectedNumPartitions;
    private ShardPartitionUtil shardPartitionUtil;

    @Override
    public int getPartition(Text id, T value, int numPartitions) {
        if (shardPartitionUtil == null) {
            if (expectedNumPartitions != null) {
                if (numPartitions != expectedNumPartitions) {
                    throw new RuntimeException("numPartitions " + numPartitions + " differed from expected " + expectedNumPartitions);
                }
            }
            shardPartitionUtil = new ShardPartitionUtil(numPartitions);
        }
        return shardPartitionUtil.shardId(id.toString());
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
        try {
            this.expectedNumPartitions = OpenSearchSnapshotOutputFormat.numShards(conf);
        } catch (Exception e) {
            throw new RuntimeException("Partitioner failed to get the expected number of shards", e);
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
