package io.github.slackhappy.opensearchsnapshotwriter;

/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2024 John Gallagher
 */


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.codelibs.opensearch.runner.OpenSearchRunner;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.repositories.RepositoryData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * OpenSearchSnapshotRecordWriterBase is part of OpenSearchSnapshotOutputFormat.
 * The expected record format is:
 *   K: string _id of document (for example String or Text)
 *   V: serialized source (for example byte[] or BytesWritable)
 * Each instance of RecordWriter represents a Shard, so it is important that
 * the OpenSearchSnapshotShardPartitioner was used to partition the documents
 * correctly.
 */
public abstract class OpenSearchSnapshotRecordWriterBase<K, V> extends RecordWriter<K, V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenSearchSnapshotRecordWriter.class);

    private final OpenSearchRunner runner;
    private Integer partition = null;
    private final String indexName;
    private final int numPartitions;
    private final Path taskOutputPath;
    private final Path jobOutputPath;
    private final RepositoryData templateRepositoryData;

    /***
     * Implementers are responsible for implementing this method to convert key and value
     * @param index index name, use this to set .index() on the created IndexRequest object
     * @param id Key parameter, use this to set id() on the created IndexRequest object
     * @param value Value parameter, use this to set source() on the IndexRequest object
     * @return IndexRequest to index to embedded shard
     */
    protected abstract IndexRequest toIndexRequest(String index, K id, V value);

    public OpenSearchSnapshotRecordWriterBase(TaskAttemptContext taskAttemptContext) {
        Configuration config = taskAttemptContext.getConfiguration();
        taskOutputPath = OpenSearchSnapshotOutputFormat.getTaskAttemptPath(taskAttemptContext);
        String basePath = new Path(taskOutputPath.getParent(), "node").toString();
        try {
            indexName = OpenSearchSnapshotOutputFormat.indexName(config);
            jobOutputPath = OpenSearchSnapshotOutputFormat.snapshotRootPath(taskAttemptContext.getJobID(), config);
            templateRepositoryData = OpenSearchSnapshotOutputFormat.outputRepositoryData(taskAttemptContext.getJobID(), config);
            numPartitions = OpenSearchSnapshotOutputFormat.numShards(config);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize " + getClass().getName(), e);
        }

        runner = OpenSearchSnapshotOutputFormat.getRunner(
                taskAttemptContext.getConfiguration(),
                basePath,
                taskOutputPath.toString());
        OpenSearchSnapshotOutputFormat.createIndex(runner, taskAttemptContext.getConfiguration(), 1, false);
    }


    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
        IndexRequest indexRequest = toIndexRequest(indexName, key, value);
        String id = indexRequest.id();
        if (id == null) {
            throw new RuntimeException("input id key is null.");
        }
        if (this.partition == null) {
            this.partition = new ShardPartitionUtil(numPartitions).shardId(id);
        }

        try {
            runner.node().client().index(indexRequest).get();
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to index id " + id, e);
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws  InterruptedException {
        if (partition == null) {
            cleanup();
            return;
        }
        try {
            Configuration config = taskAttemptContext.getConfiguration();
            FileSystem fs = taskOutputPath.getFileSystem(config);
            URI taskOutputUri = fs.resolvePath(taskOutputPath).toUri();
            OpenSearchSnapshotOutputFormat.takeSnapshot(runner, taskOutputPath.toString());
            RepositoryData taskRepositoryData = OpenSearchSnapshotOutputFormat.repositoryData(taskOutputPath, config);
            SnapshotRepositoryData taskSnapshotRepositoryData = new SnapshotRepositoryData(taskRepositoryData, indexName);
            Map<Path, Path> toCopy = new HashMap<>();
            RemoteIterator<LocatedFileStatus> filesIterator = fs.listFiles(taskOutputPath, true);
            SnapshotRepositoryData outputSnapshotRepositoryData = new SnapshotRepositoryData(templateRepositoryData, indexName);
            while (filesIterator.hasNext()) {
                LocatedFileStatus fileStatus = filesIterator.next();
                Path srcPath = fs.resolvePath(fileStatus.getPath());
                Path destPath = outputSnapshotRepositoryData.rename(taskSnapshotRepositoryData, srcPath, partition);
                if (destPath != null) {
                    Path relativePath = new Path(taskOutputUri.relativize(destPath.toUri()));
                    Path jobDestPath = new Path(jobOutputPath, relativePath);
                    toCopy.put(srcPath, jobDestPath);
                }
            }
            FileSystem destFs = jobOutputPath.getFileSystem(config);
            toCopy.forEach((srcPath, destPath) -> {
                try {
                    LOGGER.info("mv {}\n   {}", srcPath, destPath);
                    // TODO: make the OutputCommitter do this?  Using FileUtil.copy is probably not efficient for s3
                    FileUtil.copy(fs, srcPath, destFs, destPath, true, true, config);

                } catch (IOException e) {
                    throw new RuntimeException("Failed to rename file " + srcPath + " to " + destPath, e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException("Failed to close", e);
        } finally {
            cleanup();
        }
    }

    private void cleanup() {
        try {
            runner.close();
            FileUtils.deleteQuietly(new File(taskOutputPath.getParent().toString()));
        } catch (Exception e) {
            LOGGER.error("Failed to cleanly close", e);
        }
    }

}
