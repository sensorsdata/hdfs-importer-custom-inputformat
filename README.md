# HDFSImporter 自定义 InputFormat

## 1. 概述

HDFSImporter 是用于将存放于 HDFS 上的数据批量地导入到 Sensors Analytics 的工具。

默认情况下，HDFSImporter读取的数据是位于 HDFS 上的文本文件，且格式符合 Sensors Analytics 的要求，数据格式相关定义请参考：

https://www.sensorsdata.cn/manual/data_schema.html

若希望使用 HDFSImporter 导入其他格式的数据，就需要开发自己的 InputFormat，处理流程大致如下：

```
         InputFormat<LongWritable, Text>
    HDFS ==============================> 符合 Sensors Analytics 的数据格式定义的数据 => mapper
```

一个例子，读取的文件格式 ORCFile，具体表结构和数据如下：

```shell
CREATE EXTERNAL TABLE rawdata.event_table_example (
        distinct_id STRING,
        `time` TIMESTAMP,
        event_name STRING,
        ip STRING,
        app_version STRING,
        user_agent STRING
) STORED AS ORC
LOCATION '/sa/tmp/orc/event_table_example/';

INSERT INTO rawdata.event_table_example VALUES('zhangsan', '2017-07-05 13:36:00.000', 'PageView', '10.0.106.25', 'V1.7.2699', 'Mozilla');
```

经过 **自定义数据预处理模块** 处理，得到 **符合 Sensors Analytics 数据格式定义的数据**，内容如下：

```json
{
    "distinct_id": "zhangsan",
    "time":1499232960000,
    "type":"track",
    "event":"PageView",
    "properties":{
        "$ip":"10.0.106.25",
        "$user_agent":"Mozilla",
        "$app_version":"V1.7.2699"
    }
}
```

`src` 代码为一个具体的示例，功能是读取 ORCFile 并转为符合 Sensors Analysis 要求的格式。

## 2. 开发方法

参考代码示例，实现自己的 InputFormat，注意必须继承自 FileInputFormat<LongWritable, Text>

类定义示意如下：

```java
public class SaOrcInputFormat extends FileInputFormat<LongWritable, Text> {
  @Override
  public RecordReader<LongWritable, Text>
      createRecordReader(InputSplit inputSplit,
                         TaskAttemptContext taskAttemptContext
                         ) throws IOException, InterruptedException {
  }
}
```

## 3. 编译启动方法

1. 编译 Java 代码并打包。以样例为例子：

   ```bash
   mvn clean package
   ```

2. 将编译出来的 `custom_inputformat-1.0-SNAPSHOT.jar` 拷贝到部署了 Sensors Analytics 服务的机器目录下，如： `/home/sa_cluster/custom_lib/custom_inputformat-1.0-SNAPSHOT.ja`。注意尽量不要放在 `/home/sa_cluster/sa/` 子目录下，否则有可能在神策系统升级的时候被覆盖掉。

3. （可选）指定环境变量和类名参数，并开启 `debug` 模式，然后启动 HDFSImporter 导入任务进行数据校验。

   ```shell
   # 通过该环境变量设置 jar 包的位置
   export CUSTOM_LIB_JARS='/home/sa_cluster/custom_lib/custom_inputformat-1.0-SNAPSHOT.jar'
   sh /home/sa_cluster/sa/tools/hdfs_importer/bin/hdfs_importer.sh \
   --path /data/your_input \
   --project your_project \
   --input_format com.sensorsdata.analytics.mapreduce.orcfile.SaOrcInputFormat \ # 通过该参数指定 InputFormat 类名
   --debug # 通过该参数开启 debug 模式,只会校验数据，不会真正的进行导入
   ```

4. 数据校验符合预期之后，就可以关掉 `debug` 模式，进行正常的数据导入了。

   ```shell
   # 通过该环境变量设置 jar 包的位置
   export CUSTOM_LIB_JARS='/home/sa_cluster/custom_lib/custom_inputformat-1.0-SNAPSHOT.jar'
   sh /home/sa_cluster/sa/tools/hdfs_importer/bin/hdfs_importer.sh \
   --path /data/your_input \
   --project your_project \
   --input_format com.sensorsdata.analytics.mapreduce.orcfile.SaOrcInputFormat # 通过该参数指定 InputFormat 类名
   ```

## 4. 其他

* 自定义的InputFormat抛出任意未被捕获的异常都有可能直接导致导入任务执行失败