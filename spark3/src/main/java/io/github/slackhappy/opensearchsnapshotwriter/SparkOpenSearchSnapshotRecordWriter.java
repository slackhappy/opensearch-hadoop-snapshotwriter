package io.github.slackhappy.opensearchsnapshotwriter;

/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2024 John Gallagher
 */

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.core.xcontent.MediaTypeRegistry;

/**
 * OpenSearchSnapshotRecordWriterBase implementation for String, byte[] instead of Text, BytesWritable
 */
public class SparkOpenSearchSnapshotRecordWriter extends OpenSearchSnapshotRecordWriterBase<String, byte[]> {

    public SparkOpenSearchSnapshotRecordWriter(TaskAttemptContext taskAttemptContext) {
        super(taskAttemptContext);
    }

    @Override
    protected IndexRequest toIndexRequest(String index, String id, byte[] value) {
        IndexRequest r = new IndexRequest(index);
        r.id(id);
        r.source(value, MediaTypeRegistry.JSON);
        return r;
    }
}
