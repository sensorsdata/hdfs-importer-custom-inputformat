/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.sensorsdata.analytics.mapreduce.rcfile;

import com.sensorsdata.analytics.mapreduce.common.SaConfigParser;
import com.sensorsdata.analytics.mapreduce.common.SaDataMapping;
import com.sensorsdata.analytics.mapreduce.exception.SaException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFile.Reader;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SaRCFileMapReduceRecordReader extends RecordReader<LongWritable, Text> {
  private static final Logger logger = LoggerFactory.getLogger(SaRCFileMapReduceRecordReader.class);

  private Reader in;
  private long start;
  private long end;
  private boolean more = true;

  // key and value objects are created once in initialize() and then reused
  // for every getCurrentKey() and getCurrentValue() call. This is important
  // since RCFile makes an assumption of this fact.

  private LongWritable key;
  private BytesRefArrayWritable value;

  private SaConfigParser saConfigParser;
  private ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return this.mapRowToJsonData(value);
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (end == start) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (in.getPosition() - start) / (float) (end - start));
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {

    more = next(key);
    if (more) {
      in.getCurrentRow(value);
    }

    return more;
  }

  private boolean next(LongWritable key) throws IOException {
    if (!more) {
      return false;
    }

    more = in.next(key);
    if (!more) {
      return false;
    }

    if (in.lastSeenSyncPos() >= end) {
      more = false;
      return more;
    }
    return more;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
    InterruptedException {

    FileSplit fSplit = (FileSplit) split;
    Path path = fSplit.getPath();
    Configuration conf = context.getConfiguration();
    this.in = new RCFile.Reader(path.getFileSystem(conf), path, conf);
    this.end = fSplit.getStart() + fSplit.getLength();

    if (fSplit.getStart() > in.getPosition()) {
      in.sync(fSplit.getStart());
    }

    this.start = in.getPosition();
    more = start < end;

    key = new LongWritable();
    value = new BytesRefArrayWritable();

    String config = context.getConfiguration().get("mappingConfigContext");
    logger.info("mapping config: [{}]", config);
    try {
      this.saConfigParser = SaConfigParser.parseFromString(config);
    } catch (SaException e) {
      logger.error("failed to parse data mapping config", e);
      throw new IOException("failed to parse config", e);
    }
  }

  private Text mapRowToJsonData(BytesRefArrayWritable row) throws IOException {
    // event or profile 数据
    Map<String, Object> jsonRecord = new HashMap<>();
    // 设置数据类型： track 表示 event 事件 （必须）
    jsonRecord.put("type", this.saConfigParser.getType());
    // 设置用户的唯一ID（必须）
    BytesRefWritable bytesRef =
        row.get(this.saConfigParser.getDistinctIdMapping().getColumnIndex());
    String distinctId = new String(bytesRef.getData(), bytesRef.getStart(), bytesRef.getLength());
    jsonRecord.put("distinct_id", distinctId);
    // 设置事件的发生时间（必须）
    bytesRef = row.get(this.saConfigParser.getTimeMapping().getColumnIndex());
    long time =
        Long.parseLong(new String(bytesRef.getData(), bytesRef.getStart(), bytesRef.getLength()));
    jsonRecord.put("time", time);
    // 设置事件的名称（对于 track 事件必须）
    if (this.saConfigParser.getEventMapping() != null) {
      bytesRef = row.get(this.saConfigParser.getEventMapping().getColumnIndex());
      String eventName = new String(bytesRef.getData(), bytesRef.getStart(), bytesRef.getLength());
      jsonRecord.put("event", eventName);
    }
    if (this.saConfigParser.isTimeFree()) {
      jsonRecord.put("time_free", true);
    }

    // 设置事件相关属性（根据需要自定义
    Map<String, Object> recordProperties = new HashMap<>();
    jsonRecord.put("properties", recordProperties);
    for(Map.Entry<String, SaDataMapping> entry :
        this.saConfigParser.getPropertiesMapping().entrySet()) {
      recordProperties.put(entry.getKey(), this.mapValueToObject(row, entry.getValue()));
    }
    return new Text(objectMapper.writeValueAsString(jsonRecord));
  }

  private Object mapValueToObject(BytesRefArrayWritable row, SaDataMapping saDataMapping)
      throws IOException {
    BytesRefWritable bytesRef = row.get(saDataMapping.getColumnIndex());
    String value = new String(bytesRef.getData(), bytesRef.getStart(), bytesRef.getLength());
    switch (saDataMapping.getDataType()) {
      case BOOL:
        return Boolean.valueOf(value);
      case STRING:
        return value;
      case NUMBER:
        return Long.parseLong(value);
      case DATE:
        return FastDateFormat.getInstance("yyyy-MM-dd").format(Long.parseLong(value));
      case DATETIME:
        return FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS").format(Long.parseLong(value));
      case LIST:
      default:
        logger.error("not supported data type: {}", saDataMapping.getDataType());
        throw new IOException("not supported data type now.");
    }
  }
}
