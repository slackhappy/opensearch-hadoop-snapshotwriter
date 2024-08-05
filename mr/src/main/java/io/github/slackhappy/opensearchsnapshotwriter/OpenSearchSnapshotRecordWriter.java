package io.github.slackhappy.opensearchsnapshotwriter;

/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2024 John Gallagher
 */

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.core.xcontent.MediaTypeRegistry;

/**
 * OpenSearchSnapshotRecordWriter is part of OpenSearchSnapshotOutputFormat.
 * The expected record format is:
 *   Text: string _id of document
 *   BytesWritable: serialized source document bytes (currently JSON format)
 * Each instance of RecordWriter represents a Shard, so it is important that
 * the OpenSearchSnapshotShardPartitioner was used to partition the documents
 * correctly.
 */
public class OpenSearchSnapshotRecordWriter extends OpenSearchSnapshotRecordWriterBase<Text, BytesWritable> {

    @Override
    protected IndexRequest toIndexRequest(String index, Text id, BytesWritable value) {
        IndexRequest r = new IndexRequest();
        r.index(index);
        r.id(id.toString());
        r.source(value.getBytes(), 0, value.getLength(), MediaTypeRegistry.JSON);
        return r;
    }

    public OpenSearchSnapshotRecordWriter(TaskAttemptContext taskAttemptContext) {
        super(taskAttemptContext);
    }
}