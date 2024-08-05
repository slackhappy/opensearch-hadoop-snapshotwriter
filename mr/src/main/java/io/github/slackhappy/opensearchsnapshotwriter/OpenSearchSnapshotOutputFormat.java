package io.github.slackhappy.opensearchsnapshotwriter;

/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2024 John Gallagher
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.codelibs.opensearch.runner.OpenSearchRunner;
import org.opensearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.snapshots.SnapshotState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.codelibs.opensearch.runner.OpenSearchRunner.newConfigs;
import static org.opensearch.repositories.blobstore.BlobStoreRepository.INDEX_FILE_PREFIX;

/**
 * OpenSearchSnapshotOutputFormat builds offline search indexes directly
 * using embedded, in-jvm, single-node instances of OpenSearch, ingests records,
 * snapshots each shard, then merges the snapshots into a complete index.
 */
public class OpenSearchSnapshotOutputFormat extends OpenSearchSnapshotOutputFormatBase<Text, BytesWritable> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenSearchSnapshotOutputFormat.class);
    public static final AtomicInteger BasePortIncrement = new AtomicInteger(0);

    /**
     * Configuration key for the output directory.  NOTE: the job id and index name will be added
     * automatically as subdirectories.  The snapshot will be in the jobid, index generated subdirs.
     */
    public static String OUTPUT_DIR_KEY = "mapreduce.output.fileoutputformat.outputdir";

    /**
     * Configuration key for the value of the index name to write to (preferred).
     */
    public static String RESOURCE_WRITE_KEY = "opensearch.resource.write";

    /**
     * Configuration key for the value of the index name to write to.
     */
    public static String RESOURCE_KEY = "opensearch.resource";

    /**
     *  Configuration key for the value of the Json document describing the index mapping.
     *  <a href="https://opensearch.org/docs/latest/api-reference/index-apis/put-mapping/#dynamic">
     *  Dynamic mapping</a> is disabled, so all fields must be declared in the mapping.
     */
    public static String MAPPING_KEY = "opensearch.mapping";

    /**
     * Configuration key for the value of the number of shards to produce for the index
     */
    public static String NUMBER_OF_SHARDS_KEY = "opensearch.number_of_shards";


    /**
     * Configuration key for the value of the comma separated list of plugins to load.
     * Note: a transport, like org.opensearch.transport.Netty4Plugin, must be in the list
     * All modules must be accessible on the classpath.
     */
    public static String MODULES_KEY = "opensearch.modules";
    /**
     * Default module to load.
     */
    public static String MODULES_DEFAULT = "org.opensearch.transport.Netty4Plugin";

    private final static String EXCEPTION_KEY_MISSING_FMT = "%s missing from job configuration." +
            " Set to configure the %s.  If in spark" +
            " Try prefixing the key with spark.hadoop.";


    /**
     * Helper for adding this OutputFormat as the OutputFormat class for the job
     * @param job job to add configuration to
     */
    public static void setOutputFormatClass(Job job) {
        job.setOutputFormatClass(OpenSearchSnapshotOutputFormat.class);
        try {
            job.setNumReduceTasks(numShards(job.getConfiguration()));
        } catch (Exception e) {
            throw new RuntimeException("Failed to set reduce tasks", e);
        }
    }

    /**
     * Helper for setting the index's mapping via a map instead of directly setting the opensearch.mapping key
     * <a href="https://opensearch.org/docs/latest/api-reference/index-apis/put-mapping/#required-request-body-field">...</a>
     * @param conf hadoop configuration
     * @param mapping mapping.  Note: should contain "properties" entry in the root.
     */
    public static void setIndexMapping(Configuration conf, Map<String, ?> mapping) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.map(mapping);
            conf.set(MAPPING_KEY, builder.toString());
        } catch (IOException e) {
            throw new RuntimeException("Failed to encode index mapping", e);
        }

    }

    @Override
    public RecordWriter<Text, BytesWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) {
        return new OpenSearchSnapshotRecordWriter(taskAttemptContext);
    }


    public static Path getTaskAttemptPath(TaskAttemptContext context) {
        return getTempOutputPath(context.getConfiguration(), context.getTaskAttemptID().toString());
    }

    public static Path getTempOutputPath(Configuration configuration, String contextId) {
        try {
            Path tmpDir = new Path(System.getProperty("java.io.tmpdir"));
            Path subDir = new Path(tmpDir, "opensearchsnapshotwriter");
            Path taskDir = new Path(subDir, contextId);
            Path outputDir = new Path(taskDir, "output");
            FileSystem fs = FileSystem.getLocal(configuration);
            fs.mkdirs(outputDir);
            return outputDir;
        } catch (IOException e) {
            throw new RuntimeException("Failed to get local attempt path", e);
        }
    }

    public static RepositoryData outputRepositoryData(JobID jobID, Configuration conf) throws IOException {
        Path jobSnapDir = snapshotRootPath(jobID, conf);
        return repositoryData(jobSnapDir, conf);
    }

    public static RepositoryData repositoryData(Path jobSnapDir, Configuration conf) throws IOException{
        try {
            FileSystem fs = jobSnapDir.getFileSystem(conf);
            long indexGen = 0;
            String snapshotIndexName = INDEX_FILE_PREFIX + indexGen;
            try (
                    FSDataInputStream inputStream = fs.open(new Path(jobSnapDir, snapshotIndexName));
                    XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, inputStream)
            ) {
                return RepositoryData.snapshotsFromXContent(parser, indexGen, true);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to get the output repository data.  "
                    + "Did OpenSearchSnapshotOutputCommitter.setupJob complete?", e);
        }
    }


    public static Path snapshotRootPath(JobID jobID, Configuration conf) throws IOException {
        Path snapshotRoot = outputDir(conf);
        Path jobPath = new Path(snapshotRoot, jobID.toString());
        return new Path(jobPath, indexName(conf));
    }

    public static String mappings(Configuration conf) throws IOException {
        String mapping = conf.get(MAPPING_KEY);
        if (mapping == null || mapping.isEmpty()) {
            throw new IOException(String.format(EXCEPTION_KEY_MISSING_FMT,
                    MAPPING_KEY,
                    "index field mapping json (dynamic mapping not allowed)"));
        }

        Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent,mapping, false);
        // Don't allow dynamic mapping
        Map<String, Object> body = map;
        if (map.size() == 1 && map.containsKey("_doc")) {
            body = (Map<String, Object>) map.get("_doc");
        }
        body.put("dynamic", "strict");
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.map(map);
            String finalMapping = builder.toString();
            conf.set(MAPPING_KEY, finalMapping);
            return finalMapping;
        }
    }

    public static Path outputDir(Configuration conf) throws IOException {
        String outputDir = conf.get(OUTPUT_DIR_KEY);
        if (outputDir == null || outputDir.isEmpty()) {
            throw new IOException(String.format(EXCEPTION_KEY_MISSING_FMT,
                    OUTPUT_DIR_KEY,
                    "snapshot output root directory (jobid and index will be added as a suffix)"));
        }
        return new Path(outputDir);
    }

    public static String indexName(Configuration conf) throws IOException {
        String indexName = conf.get(RESOURCE_WRITE_KEY);
        if (indexName == null) {
            indexName = conf.get(RESOURCE_KEY);
        }
        if (indexName == null) {
            throw new IOException(String.format(EXCEPTION_KEY_MISSING_FMT,
                    RESOURCE_WRITE_KEY + " or " + RESOURCE_KEY,
                    "index name"));
        }
        return indexName;
    }

    public static int numShards(Configuration conf) throws IOException {
        String numShardsStr = conf.get(NUMBER_OF_SHARDS_KEY);
        if (numShardsStr == null) {
            throw new IOException(String.format(EXCEPTION_KEY_MISSING_FMT,
                    NUMBER_OF_SHARDS_KEY,
                    "number of shards in the index (index.number_of_shards)"));
        }
        int numShards = conf.getInt(NUMBER_OF_SHARDS_KEY, -1);
        if (numShards < 1) {
            throw new IOException(NUMBER_OF_SHARDS_KEY + " must be a valid number >= 1");
        }
        return numShards;
    }

    public static OpenSearchRunner getRunner(Configuration conf, String basePath, String snapPath) {
        OpenSearchRunner runner = new OpenSearchRunner();
        OpenSearchRunner.Configs runnerConfig = newConfigs()
                .numOfNode(1)
                .basePath(basePath)
                .disableESLogger()
                .baseHttpPort(9200 + BasePortIncrement.getAndAdd(2))
                .moduleTypes(conf.get(MODULES_KEY, MODULES_DEFAULT));
        runner.onBuild(new OpenSearchRunner.Builder() {
            public void build(int index, Settings.Builder builder) {
                System.setProperty("opensearch.set.netty.runtime.available.processors", "false");
                builder.put("discovery.type", "single-node");
                builder.put("action.auto_create_index", "false");
                // Optionally define custom behavior on OpenSearchRunner build
                if (snapPath != null) {
                    builder.put("path.repo", snapPath);
                }
            }
        }).build(runnerConfig);
        return runner;
    }


    public static String createIndex(OpenSearchRunner runner, Configuration configuration, int numShards, boolean isTemplate) {
        try {
            String mapping = configuration.get(MAPPING_KEY);
            String indexName = indexName(configuration);
            Settings.Builder settingsBuilder = Settings.builder()
                .put("index.number_of_replicas", 0)
                .put("index.number_of_shards", numShards)
                // TODO: consider using MetadataCreateIndexService.calculateNumRoutingShards
                .put("index.number_of_routing_shards", numShards);

            // When building the shards, disable refresh interval and translog syncing
            if (!isTemplate) {
                // No need to auto refresh
                settingsBuilder.put("index.refresh_interval", -1);
                // No need for sync persistence
                settingsBuilder.put("index.translog.durability", "async");
            }

            CreateIndexResponse res = runner.node().client().admin().indices().prepareCreate(indexName)
                    .setSettings(settingsBuilder)
                    .setMapping(mapping)
                    .execute().get();
            if (!res.isAcknowledged()) {
                throw new RuntimeException("Create index command was not acknowledged");
            }
            return indexName;
        } catch (ExecutionException | InterruptedException | IOException e) {
            throw new RuntimeException("Failed to create index", e);
        }
    }

    public static CreateSnapshotResponse takeSnapshot(OpenSearchRunner runner, String snapPath) {
        try {
            PutRepositoryRequest putRepo = new PutRepositoryRequest("repo");
            putRepo.type("fs");
            putRepo.settings(Settings.builder()
                    .put("location", snapPath)
                    .build()
            );
            AcknowledgedResponse putRes = runner.client().admin().cluster().putRepository(putRepo).get();
            if (!putRes.isAcknowledged()) {
                throw new RuntimeException("put repo for " + snapPath + " was not successful");
            }
            CreateSnapshotRequest createSnapshot = new CreateSnapshotRequest("repo", "snap");
            createSnapshot.waitForCompletion(true);
            CreateSnapshotResponse res = runner.client().admin().cluster().createSnapshot(createSnapshot).get();
            if (res.getSnapshotInfo().state() != SnapshotState.SUCCESS) {
                throw new RuntimeException("snapshot was to " + snapPath + " was not successful: " + res);
            }
            return res;
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException("Failed to take snapshot", e);
        }
    }
}