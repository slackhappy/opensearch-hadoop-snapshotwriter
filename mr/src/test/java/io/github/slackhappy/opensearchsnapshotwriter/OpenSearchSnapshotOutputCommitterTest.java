package io.github.slackhappy.opensearchsnapshotwriter;

/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2024 John Gallagher
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.lucene.search.TotalHits;
import org.codelibs.opensearch.runner.OpenSearchRunner;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.search.SearchHits;

import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class OpenSearchSnapshotOutputCommitterTest {
    @Test
    public void testSetupJob() throws Exception {
        String indexName = "test";
        Configuration conf = new Configuration(false);
        conf.set("fs.default.name", "file:///");
        String pathPrefix = getClass().getSimpleName();
        Path tmpOutDir = null;
        Path tmpInDir = null;
        Path tmpVerifyDir = null;
        int numDocs = 1000;
        int numShards = 2;
        try {
            tmpOutDir = Files.createTempDirectory(pathPrefix + "-output");
            tmpInDir = Files.createTempDirectory(pathPrefix + "-input");
            try (FileWriter fw = new FileWriter(tmpInDir.resolve("input.jsonl").toFile())) {
                ObjectMapper objectMapper = new ObjectMapper();
                Map<String, Object> objectMap = new HashMap<>();
                for (int i = 0; i < numDocs; i++) {
                    objectMap.put("_id", "id" + i);
                    objectMap.put("text", "foo " + i);
                    objectMap.put("docid", i);
                    fw.append(objectMapper.writeValueAsString(objectMap));
                    fw.append("\n");
                }
            }
            conf.set(OpenSearchSnapshotOutputFormat.OUTPUT_DIR_KEY, tmpOutDir.toString());
            conf.set(OpenSearchSnapshotOutputFormat.RESOURCE_KEY, indexName);
            conf.set(OpenSearchSnapshotOutputFormat.NUMBER_OF_SHARDS_KEY, String.valueOf(numShards));
            Map<String, Object> fieldsMap = new HashMap<>();
            fieldsMap.put("text", Collections.singletonMap("type", "text"));
            fieldsMap.put("docid", Collections.singletonMap("type", "long"));
            Map<String, Object> mappingsMap = Collections.singletonMap("properties", fieldsMap);
            OpenSearchSnapshotOutputFormat.setIndexMapping(conf, mappingsMap);


            Job job = new Job(conf, getClass().getName());
            job.setInputFormatClass(TextInputFormat.class);

            TextInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(tmpInDir.toString()));
            OpenSearchSnapshotJsonlMapper.setMapperClass(job);
            OpenSearchSnapshotOutputFormat.setOutputFormatClass(job);

            job.setPartitionerClass(OpenSearchSnapshotShardPartitioner.class);

            job.setJobID(new JobID());
            Assert.assertTrue("job was successful", job.waitForCompletion(false));

            RepositoryData data = OpenSearchSnapshotOutputFormat.outputRepositoryData(job.getJobID(), conf);
            Assert.assertEquals(1, data.getIndices().size());
            Assert.assertEquals(0L, data.getGenId());


            tmpVerifyDir = Files.createTempDirectory(pathPrefix + "-verify");
            String outputSnapshotDir = OpenSearchSnapshotOutputFormat.snapshotRootPath(job.getJobID(), conf).toString();
            OpenSearchRunner runner = OpenSearchSnapshotOutputFormat.getRunner(conf, tmpVerifyDir.toString(), outputSnapshotDir);
            PutRepositoryRequest putRepo = new PutRepositoryRequest("repo");
            putRepo.type("fs");
            putRepo.settings(Settings.builder()
                    .put("location", outputSnapshotDir)
                    .build()
            );
            runner.client().admin().cluster().putRepository(putRepo).get();
            runner.client().admin().cluster().prepareRestoreSnapshot("repo", "snap").setWaitForCompletion(true).get();
            SearchResponse res = runner.client().prepareSearch(indexName).get();
            SearchHits hits = res.getHits();
            Assert.assertNotNull(hits);
            TotalHits totalHits = hits.getTotalHits();
            Assert.assertNotNull(totalHits);
            Assert.assertEquals("hits.total", numDocs, totalHits.value);
            Assert.assertEquals("shards.total", numShards, res.getTotalShards());
            Assert.assertEquals("shards.successful", numShards, res.getSuccessfulShards());


        } finally {
            for (Path dir : new Path[] { tmpInDir, tmpOutDir, tmpVerifyDir}) {
                if (dir != null) {
                    FileUtils.deleteQuietly(dir.toFile());
                }
            }
        }
    }
}
