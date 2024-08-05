package io.github.slackhappy.opensearchsnapshotwriter;

/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2024 John Gallagher
 */

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A simple, example Jsonl mapper that can process lines of json objects.
 * The id field will be extracted as the key, and the remaining object becomes the source
 */
public class OpenSearchSnapshotJsonlMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {
    /**
     * Field to extract as the value for the id (_id is the default)
     */
    public static String ID_FIELD_KEY = "opensearch.mapping.id";
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TypeReference<HashMap<String, Object>> hashMapType = new TypeReference<>() {};
    private String idField = "_id";
    boolean removeIdField = true;

    public static void setMapperClass(Job job) {
        job.setMapperClass(OpenSearchSnapshotJsonlMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
    }

    protected void setup(Context context) {
        idField = context.getConfiguration().get(ID_FIELD_KEY, idField);
        removeIdField = idField.equals("_id");
    }
    public void map(LongWritable lineNo, Text document, Context context) throws IOException, InterruptedException {
        String line = document.toString().trim();
        if (line.isEmpty()) {
            return;
        }

        Map<String, Object> documentMap = objectMapper.readValue(line, hashMapType);
        IndexRequest r = new IndexRequest();
        if (!documentMap.containsKey(idField)) {
            throw new IOException("Document on line " + lineNo.toString() +
                    " from " + context.getInputSplit() + " missing id field " + idField);
        }
        Text id = new Text(documentMap.get(idField).toString());
        if (removeIdField) {
            documentMap.remove(idField);
        }
        r.source(documentMap);
        BytesWritable source = new BytesWritable(BytesReference.toBytes(r.source()));
        context.write(id, source);
    }
}
