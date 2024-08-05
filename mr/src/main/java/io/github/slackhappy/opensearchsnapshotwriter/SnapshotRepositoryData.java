package io.github.slackhappy.opensearchsnapshotwriter;

/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2024 John Gallagher
 */


import org.apache.hadoop.fs.Path;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.snapshots.SnapshotId;

import java.util.List;

/**
 * A wrapper around RepositoryData that specifically helps
 * with renaming shard snapshot files to match the all-shard template
 * snapshot produced by the OpenSearchSnapshotOutputCommitter
 */
public class SnapshotRepositoryData {
    private final RepositoryData repositoryData;
    private final String indexName;
    public SnapshotRepositoryData(RepositoryData repositoryData, String indexName) {
        this.indexName = indexName;
        this.repositoryData = repositoryData;
    }

    public RepositoryData repositoryData() {
        return repositoryData;
    }

    public IndexId indexId() {
        return repositoryData.getIndices().get(indexName);
    }

    public String indexUUID() {
        return indexId().getId();
    }

    public String snapshotUUID() {
        List<SnapshotId> snapshotIds = repositoryData.getSnapshots(indexId());
        if (snapshotIds.size() != 1) {
            throw new RuntimeException("unexpected number of snapshot ids " + snapshotIds.size());
        }
        return snapshotIds.get(0).getUUID();
    }

    public String shardGenerationId(int shardNum) {
        return repositoryData.shardGenerations().getGens(indexId()).get(shardNum);
    }

    public Path rename(SnapshotRepositoryData src, Path srcPath, int destShardNum) {
        Path expected = new Path(new Path("indices", src.indexUUID()), "0");
        // Skip non-shard data, like repository and index metadata.
        String path = srcPath.toString();
        if (!path.contains(expected.toString())) {
            return null;
        }
        Path replaceWith = new Path(new Path("indices", indexUUID()), String.valueOf(destShardNum));
        path = path.replace(expected.toString(), replaceWith.toString());
        path = path.replace(src.snapshotUUID(), snapshotUUID());
        path = path.replace(src.shardGenerationId(0), shardGenerationId(destShardNum));
        return new Path(path);
    }
}
