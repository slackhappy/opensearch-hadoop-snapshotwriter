package io.github.slackhappy.opensearchsnapshotwriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

abstract public class OpenSearchSnapshotOutputFormatBase<K, V> extends OutputFormat<K, V> {
    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException {
        // Attempt to extract key parameters
        Configuration conf = jobContext.getConfiguration();
        Path rootPath = OpenSearchSnapshotOutputFormat.snapshotRootPath(jobContext.getJobID(), conf);
        if (rootPath.getFileSystem(conf).exists(rootPath)) {
            throw new IOException("path " + rootPath + "already exists");
        }
        OpenSearchSnapshotOutputFormat.indexName(conf);
        OpenSearchSnapshotOutputFormat.numShards(conf);
        OpenSearchSnapshotOutputFormat.mappings(conf);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) {
        return new OpenSearchSnapshotOutputCommitter();
    }
}
