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
import org.apache.commons.lang3.StringUtils;
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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SaRCFileMapReduceRecordReader extends RecordReader<LongWritable, Text> {
  private static final Logger logger =
      LoggerFactory.getLogger(SaRCFileMapReduceRecordReader.class);
  private static final FastDateFormat FULL_DATETIME_FORMAT =
      FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.S");
  private static final FastDateFormat DEFAULT_DAY_FORMAT =
      FastDateFormat.getInstance("yyyy-MM-dd");

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

  private Date dayPartition;

  SaRCFileMapReduceRecordReader(String dayPartition) throws InterruptedException {
    try {
      this.dayPartition = DEFAULT_DAY_FORMAT.parse(dayPartition);
    } catch (ParseException e) {
      logger.error("failed to parse day partition: {}", dayPartition);
      throw new InterruptedException("failed to parse day partition");
    }
  }

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
    Map<String, Object> record = new HashMap<>();
    // 设置数据类型： track 表示 event 事件 （必须）
    record.put("type", this.saConfigParser.getType());
    // 设置用户的唯一ID（必须）
    String distinctId =
        this.getRequiredColumn(row, this.saConfigParser.getDistinctIdMapping().getColumnIndex());
    record.put("distinct_id", distinctId);
    // 只有 event 数据需要使用真实的时间，profile 可以使用发送时间
    if (this.saConfigParser.getTimeMapping().getColumnIndex() >= 0) {
      String eventTime =
          this.getRequiredColumn(row, this.saConfigParser.getTimeMapping().getColumnIndex());
      try {
        Date baseTime = FULL_DATETIME_FORMAT.parse(eventTime);
        long realTime = this.calculateRealTime(baseTime, this.dayPartition);
        /*if (this.saConfigParser.getDayMapping() != null) {
          String dayStr =
              this.getRequiredColumn(row, this.saConfigParser.getDayMapping().getColumnIndex());
          realTime = this.calculateRealTime(baseTime, DEFAULT_DAY_FORMAT.parse(dayStr));
        } else {
          realTime = baseTime.getTime();
        }*/
        record.put("time", realTime);
      } catch (ParseException e) {
        logger.error("failed to parse event time: {}", eventTime);
      }
    } else {
      record.put("time", 0);
    }
    // 设置事件的名称（对于 track 事件必须）
    if (this.saConfigParser.getEventMapping() != null) {
      String eventName =
          this.getRequiredColumn(row, this.saConfigParser.getEventMapping().getColumnIndex());
      record.put("event", eventName);
    }
    if (this.saConfigParser.isTimeFree()) {
      record.put("time_free", true);
    }

    // 设置事件相关属性（根据需要自定义
    Map<String, Object> recordProperties = new HashMap<>();
    Map<String, Object> unsetProperties = new HashMap<>();
    record.put("properties", recordProperties);
    for(Map.Entry<String, SaDataMapping> entry :
        this.saConfigParser.getPropertiesMapping().entrySet()) {
      Object value = this.mapValueToObject(row, entry.getValue());
      if (value != null) {
        recordProperties.put(entry.getKey(), value);
      } else {
        unsetProperties.put(entry.getKey(), true);
      }
    }

    // 一次可以返回多条数据（可以是事件和用户属性）
    List<Map<String, Object>> records = new ArrayList<>();

    // 对于profile 产生 profile_set 和 profile_unset 两条
    // 对于 profile_set 会同时生成一条 profile_unset, 将所有的属性设置为空
    if (this.saConfigParser.getType().equals("profile_set")) {
      Map<String, Object> unsetRecord = new HashMap<>();
      unsetRecord.put("type", "profile_unset");
      unsetRecord.put("distinct_id", distinctId);
      unsetRecord.put("properties", unsetProperties);
      records.add(unsetRecord);
    }

    records.add(record);
    return new Text(objectMapper.writeValueAsString(records));
  }

  private long calculateRealTime(Date baseTime, Date dayTime) {
    Calendar baseCalendar = Calendar.getInstance();
    baseCalendar.setTime(baseTime);

    Calendar dayCalendar = Calendar.getInstance();
    dayCalendar.setTime(dayTime);

    baseCalendar.set(Calendar.YEAR, dayCalendar.get(Calendar.YEAR));
    baseCalendar.set(Calendar.MONTH, dayCalendar.get(Calendar.MONTH));
    baseCalendar.set(Calendar.DATE, dayCalendar.get(Calendar.DATE));

    return baseCalendar.getTimeInMillis();
  }

  private String getRequiredColumn(BytesRefArrayWritable row, int index) throws IOException {
    BytesRefWritable bytesRef = row.get(index);
    String value = new String(bytesRef.getData(), bytesRef.getStart(), bytesRef.getLength());
    if (value.isEmpty() || value.equals("\\N")) {
      logger.error("required column can not be null, index={}, value={}", index, value);
      return "";
    }
    return value;
  }

  private Object mapValueToObject(BytesRefArrayWritable row, SaDataMapping saDataMapping)
      throws IOException {
    BytesRefWritable bytesRef = row.get(saDataMapping.getColumnIndex());
    String value = new String(bytesRef.getData(), bytesRef.getStart(), bytesRef.getLength());
    if (value.isEmpty() || value.equals("\\N")) { // NULL value
      return null;
    }
    switch (saDataMapping.getDataType()) {
      case BOOL:
        return Boolean.valueOf(value);
      case STRING:
        return value;
      case NUMBER:
        try {
          return Double.parseDouble(value);
        } catch (Exception e) {
          return null;
        }
      case DATE:
        return value;
      case DATETIME:
        return value;
      case LIST:
        return value;
      default:
        logger.error("not supported data type: {}", saDataMapping.getDataType());
        throw new IOException("not supported data type now.");
    }
  }
}
