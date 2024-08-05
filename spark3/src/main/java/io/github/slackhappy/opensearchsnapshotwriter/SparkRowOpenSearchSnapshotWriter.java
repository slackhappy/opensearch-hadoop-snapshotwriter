package io.github.slackhappy.opensearchsnapshotwriter;

/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2024 John Gallagher
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.opensearch.action.index.IndexRequest;
import scala.Tuple2;
import java.util.HashMap;
import java.util.Map;

/**
 * A RDD[Row] to OpenSearch snapshot writer. It uses
 * OpenSearchSnapshotOutputFormat to build offline search indexes directly
 * using embedded, in-jvm, single-node instances of OpenSearch, ingests records,
 * snapshots each shard, then merges the snapshots into a complete index.
 *
 */
public class SparkRowOpenSearchSnapshotWriter {
    private final JavaRDD<Row> rdd;

    public SparkRowOpenSearchSnapshotWriter(RDD<Row> rdd) {
        this.rdd = rdd.toJavaRDD();
    }

    public void snapshotForOpenSearch(String outputPath, String indexName, String mapping, int numShards) {
        // TODO: make it possible to infer mapping from row
        snapshotForOpenSearch(outputPath, indexName, mapping, numShards, "_id");
    }

    public void snapshotForOpenSearch(String outputPath, String indexName, String mapping, int numShards, String idField) {
        JavaPairRDD<String, byte[]> pairRDD = rdd.mapToPair(row -> {
            String id = null;
            Map<String, Object> data = new HashMap<>();
            for (int idx = 0; idx < row.schema().length(); idx++) {
                String fieldName = row.schema().apply(idx).name();
                if (fieldName.equals(idField)) {
                    id = row.getString(idx);
                } else {
                    data.put(fieldName, row.get(idx));
                }
            }
            IndexRequest r = new IndexRequest();
            r.source(data);
            byte[] contentBytes = r.source().toBytesRef().bytes;
            return new Tuple2<>(id, contentBytes);
        });
        JavaPairRDD<String, byte[]> repartitioned = pairRDD.partitionBy(new SparkOpenSearchSnapshotShardPartitioner(numShards));
        Configuration config = rdd.context().hadoopConfiguration();
        config.set(OpenSearchSnapshotOutputFormat.NUMBER_OF_SHARDS_KEY, String.valueOf(numShards));
        config.set(OpenSearchSnapshotOutputFormat.RESOURCE_WRITE_KEY, indexName);
        config.set(OpenSearchSnapshotOutputFormat.MAPPING_KEY, mapping);
        repartitioned.saveAsNewAPIHadoopFile(outputPath, String.class, byte[].class, SparkOpenSearchSnapshotOutputFormat.class);
    }
}