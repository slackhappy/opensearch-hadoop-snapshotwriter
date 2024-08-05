package io.github.slackhappy.opensearchsnapshotwriter;

/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2024 John Gallagher
 */

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.core.xcontent.MediaTypeRegistry;

import java.io.IOException;

/**
 * OpenSearchSnapshotOutputFormat builds offline search indexes directly
 * using embedded, in-jvm, single-node instances of OpenSearch, ingests records,
 * snapshots each shard, then merges the snapshots into a complete index.
 * SparkOpenSearchSnapshotOutputFormat uses String, byte[] instead of Text, BytesWritable
 */

public class SparkOpenSearchSnapshotOutputFormat extends OpenSearchSnapshotOutputFormatBase<String, byte[]> {
    @Override
    public RecordWriter<String, byte[]> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new OpenSearchSnapshotRecordWriterBase<>(taskAttemptContext) {
            @Override
            protected IndexRequest toIndexRequest(String index, String id, byte[] value) {
                IndexRequest r = new IndexRequest(index);
                r.id(id);
                r.source(value, MediaTypeRegistry.JSON);
                return r;
            }
        };
    }
}
