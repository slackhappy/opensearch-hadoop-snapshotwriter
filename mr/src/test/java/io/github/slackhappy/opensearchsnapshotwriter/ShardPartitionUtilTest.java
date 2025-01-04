package io.github.slackhappy.opensearchsnapshotwriter;

/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2024 John Gallagher
 */

import org.junit.Assert;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;

public class ShardPartitionUtilTest {

    @Test
    public void testPartitioner() {
        ShardPartitionUtil p = new ShardPartitionUtil(2);
        Map<String, Integer> expected = new HashMap<>();
        expected.put("id1", 1);
        expected.put("id2", 1);
        expected.put("id3", 0);
        for (Map.Entry<String, Integer> e : expected.entrySet()) {
            Assert.assertEquals("id: " + e.getKey(), e.getValue(), p.shardId(e.getKey()));
        }
    }

    @Test
    public void testGetNumberOfRoutingShards() {
        Assert.assertEquals(1024, ShardPartitionUtil.getNumberOfRoutingShards(2));
        Assert.assertEquals(1024, ShardPartitionUtil.getNumberOfRoutingShards(32));
        // Default max is 1024.  Above that, a min of 2x shards
        Assert.assertEquals(2048, ShardPartitionUtil.getNumberOfRoutingShards(1024));

    }
}