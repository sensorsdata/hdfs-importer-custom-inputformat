/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */
package com.sensorsdata.analytics.mapreduce;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcMapredRecordReader;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This record reader implements the org.apache.hadoop.mapreduce API.
 * It is in the org.apache.orc.mapred package to share implementation with
 * the mapred API record reader.
 */
public class SaOrcMapreduceRecordReader
    extends org.apache.hadoop.mapreduce.RecordReader<LongWritable, Text> {
  private static final Logger logger = LoggerFactory.getLogger(SaOrcMapreduceRecordReader.class);
  private final TypeDescription schema;
  private final RecordReader batchReader;
  private final VectorizedRowBatch batch;
  private int rowInBatch;
  private final WritableComparable row;
  private LongWritable key = new LongWritable(0);
  private ObjectMapper objectMapper = new ObjectMapper();

  public SaOrcMapreduceRecordReader(RecordReader reader,
                                  TypeDescription schema) throws IOException {
    this.batchReader = reader;
    this.batch = schema.createRowBatch();
    this.schema = schema;
    rowInBatch = 0;
    this.row = OrcStruct.createValue(schema);
  }

  SaOrcMapreduceRecordReader(Reader fileReader, Reader.Options options) throws IOException {
    this.batchReader = fileReader.rows(options);
    if (options.getSchema() == null) {
      schema = fileReader.getSchema();
    } else {
      schema = options.getSchema();
    }
    this.batch = schema.createRowBatch();
    rowInBatch = 0;
    this.row = OrcStruct.createValue(schema);
  }

  /**
   * If the current batch is empty, get a new one.
   * @return true if we have rows available.
   * @throws IOException io exception
   */
  private boolean ensureBatch() throws IOException {
    if (rowInBatch >= batch.size) {
      rowInBatch = 0;
      return batchReader.nextBatch(batch);
    }
    return true;
  }

  @Override
  public void close() throws IOException {
    batchReader.close();
  }

  @Override
  public void initialize(InputSplit inputSplit,
                         TaskAttemptContext taskAttemptContext) {
    // nothing required
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!ensureBatch()) {
      return false;
    }
    if (schema.getCategory() == TypeDescription.Category.STRUCT) {
      OrcStruct result = (OrcStruct) row;
      List<TypeDescription> children = schema.getChildren();
      int numberOfChildren = children.size();
      for(int i=0; i < numberOfChildren; ++i) {
        result.setFieldValue(i, OrcMapredRecordReader.nextValue(batch.cols[i], rowInBatch,
            children.get(i), result.getFieldValue(i)));
      }
    } else {
      OrcMapredRecordReader.nextValue(batch.cols[0], rowInBatch, schema, row);
    }
    rowInBatch += 1;
    return true;
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    this.key.set(this.key.get() + 1);
    return this.key;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return mapRowToEventData(row);
  }

  @Override
  public float getProgress() throws IOException {
    return batchReader.getProgress();
  }

  private Text mapRowToEventData(WritableComparable row) throws IOException {
    if (schema.getCategory() != TypeDescription.Category.STRUCT) {
      throw new IOException("this example only support orc struct now.");
    }

    OrcStruct result = (OrcStruct) row;
    TypeDescription typeDesc = result.getSchema();
    logger.error("field names: {}", Joiner.on(",").join(typeDesc.getFieldNames()));

    // 一条事件类型数据, 用于记录用户访问事件
    Map<String, Object> eventRecord = new HashMap<>();
    eventRecord.put("type", "track");
    // 设置用户的唯一ID
    String distinctId = result.getFieldValue(0).toString();
    eventRecord.put("distinct_id", distinctId);
    // 设置事件的发生时间
    long time = ((OrcTimestamp)result.getFieldValue(1)).getTime();
    eventRecord.put("time", time);
    // 设置事件的名称
    String eventName = result.getFieldValue(2).toString();
    eventRecord.put("event", eventName);

    // 设置事件相关属性
    Map<String, Object> eventRecordProperties = new HashMap<>();
    eventRecord.put("properties", eventRecordProperties);
    // 设置 $ip, 可解析地理位置
    eventRecordProperties.put("$ip", result.getFieldValue(3).toString());
    eventRecordProperties.put("$app_version",
        result.getFieldValue(4).toString());
    // 设置 $user_agent, 可以自动解析
    eventRecordProperties.put("$user_agent",
        result.getFieldValue(5).toString());
    // 设置其它属性

    return new Text(objectMapper.writeValueAsString(eventRecord));
  }
}
