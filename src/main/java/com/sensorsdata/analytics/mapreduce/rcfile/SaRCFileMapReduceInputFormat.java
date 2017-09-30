/**
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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class SaRCFileMapReduceInputFormat extends FileInputFormat<LongWritable, Text> {

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
                                        TaskAttemptContext context)
      throws IOException, InterruptedException {
    context.setStatus(split.toString());
    return new SaRCFileMapReduceRecordReader(parsePartitionDay(split));
  }

  private String parsePartitionDay(InputSplit inputSplit) {
    String filePathString = ((FileSplit) inputSplit).getPath().toString();
    String partitionStr = new Path(filePathString).getParent().getName();
    String defaultPartitionStr = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    if (partitionStr.contains("=")) {
      return partitionStr.split("=")[1];
    } else {
      return defaultPartitionStr;
    }
  }



  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    return super.getSplits(job);
  }
}
