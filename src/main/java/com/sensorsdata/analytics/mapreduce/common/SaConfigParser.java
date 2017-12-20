package com.sensorsdata.analytics.mapreduce.common;

import com.sensorsdata.analytics.mapreduce.exception.SaException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 解析配置文件，确定 hive 数据和 SA 数据的映射关系
 * Created by zhangguangqiang on 2017/8/9.
 */
public class SaConfigParser {
  private static final Logger logger = LoggerFactory.getLogger(SaConfigParser.class);

  // 支持 track 、profile_set 、 track_signup 等
  private String type;
  private SaDataMapping distinctIdMapping;
  private SaDataMapping timeMapping;

  // 【可选】是否采用特殊的时间设置方式
  private boolean isSpecialTime;

  // Event 必须字段
  private SaDataMapping eventMapping;
  // 是否限制 event 数据时间
  private boolean timeFree;

  // 可选: 属性字段
  private Map<String, SaDataMapping> propertiesMapping = new HashMap<>();

  // 通过 java 的默认配置进行解析
  public static  SaConfigParser parseFromString(String config) throws SaException, IOException {
    Properties properties = new Properties();
    properties.load(new StringReader(config));
    String type = properties.getProperty("type");
    SaConfigParser saConfigParser = new SaConfigParser();
    switch (type) {
      case "track":
        saConfigParser.eventMapping =
            parseRequiredMapping("event", DataType.STRING, properties);
        saConfigParser.timeFree =
            Boolean.parseBoolean(properties.getProperty("time_free", "False"));
        saConfigParser.isSpecialTime = false;
        String specialTime = properties.getProperty("special_time");
        if (specialTime != null && Integer.parseInt(specialTime) > 0) {
          saConfigParser.isSpecialTime = true;
        }
      case "profile_set":
        saConfigParser.type = type;
        saConfigParser.distinctIdMapping =
            parseRequiredMapping("distinct_id", DataType.STRING, properties);
        saConfigParser.timeMapping =
            parseRequiredMapping("time", DataType.DATETIME, properties);
        // parse properties
        String propertyStr = properties.getProperty("properties");
        if (propertyStr != null) {
          for (String mappingStr : StringUtils.split(propertyStr, ";")) {
            SaDataMapping saDataMapping = SaDataMapping.parseFromString(mappingStr.trim());
            saConfigParser.propertiesMapping.put(saDataMapping.getColumnName(), saDataMapping);
          }
        }
        break;
      default:
        logger.error("unknown type for config: {}", type);
        throw new SaException("unknown type for config: " + type);
    }
    return saConfigParser;
  }

  private static SaDataMapping parseRequiredMapping(
      String name, DataType dataType, Properties properties) throws SaException {
    String indexStr = properties.getProperty(name);
    if (indexStr == null) {
      logger.error("required column index not found for: {}", name);
      throw new SaException("required column index not found");
    }
    Integer index = Integer.parseInt(indexStr);
    return new SaDataMapping(name, index, dataType);
  }

  public String getType() {
    return type;
  }

  public SaDataMapping getDistinctIdMapping() {
    return distinctIdMapping;
  }

  public SaDataMapping getTimeMapping() {
    return timeMapping;
  }

  public SaDataMapping getEventMapping() {
    return eventMapping;
  }

  public boolean isTimeFree() {
    return timeFree;
  }

  public boolean isSpecialTime() {
    return isSpecialTime;
  }

  public Map<String, SaDataMapping> getPropertiesMapping() {
    return propertiesMapping;
  }
}
