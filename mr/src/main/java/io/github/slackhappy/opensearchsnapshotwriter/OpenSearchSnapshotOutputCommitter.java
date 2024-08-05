package io.github.slackhappy.opensearchsnapshotwriter;

/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2024 John Gallagher
 */

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.codelibs.opensearch.runner.OpenSearchRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * OpenSearchSnapshotOutputCommitter is part of OpenSearchSnapshotOutputFormat.
 * This OutputCommitter sets up the "template" snapshot layout in the output directory
 * that all output tasks contribute to
 */
class OpenSearchSnapshotOutputCommitter extends OutputCommitter {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenSearchSnapshotOutputCommitter.class);

    @Override
    public void setupTask(TaskAttemptContext taskAttemptContext) {}

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) {
        return false;
    }

    @Override
    public void commitTask(TaskAttemptContext taskAttemptContext) {}

    @Override
    public void setupJob(JobContext context) throws IOException {
        Path tempOutputPath = null;
        try {
            Configuration config = context.getConfiguration();
            Path jobOutputPath = OpenSearchSnapshotOutputFormat.snapshotRootPath(context.getJobID(), config);
            String contextId = context.getJobID().toString();
            tempOutputPath = OpenSearchSnapshotOutputFormat.getTempOutputPath(config, contextId);
            String basePath = new Path(tempOutputPath.getParent(), "node").toString();
            String snapPath = tempOutputPath.toString();
            OpenSearchRunner runner = OpenSearchSnapshotOutputFormat.getRunner(config, basePath, snapPath);
            int numShards = OpenSearchSnapshotOutputFormat.numShards(config);
            String index = OpenSearchSnapshotOutputFormat.createIndex(runner, config, numShards, true);
            LOGGER.info("Created a snapshot template for {}", index);
            OpenSearchSnapshotOutputFormat.takeSnapshot(runner, snapPath);
            FileSystem srcFs = tempOutputPath.getFileSystem(config);
            FileSystem destFs = jobOutputPath.getFileSystem(config);
            // Move files to final location
            FileUtil.copy(srcFs, tempOutputPath, destFs, jobOutputPath, true, config);
        } finally {
            if (tempOutputPath != null) {
                FileUtils.deleteQuietly(new File(tempOutputPath.getParent().toString()));
            }
        }
    }

    @Override
    public void abortTask(TaskAttemptContext taskAttemptContext) {}

    @Override
    public void commitJob(JobContext jobContext) {
        // TODO: if dynamic mapping allowed, would be a good place to merge mappings
    }
}
