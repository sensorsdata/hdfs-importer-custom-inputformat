package com.sensorsdata.analytics.mapreduce.common;

import com.sensorsdata.analytics.mapreduce.exception.SaException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 指定数据映射关系
 * Created by zhangguangqiang on 2017/8/9.
 */
public class SaDataMapping {
  private static final Logger logger = LoggerFactory.getLogger(SaDataMapping.class);

  private String columnName;
  private Integer columnIndex;
  private DataType dataType;

  SaDataMapping(String columnName, Integer columnIndex, DataType dataType) {
    this.columnName = columnName;
    this.columnIndex = columnIndex;
    this.dataType = dataType;
  }

  static SaDataMapping parseFromString(String value) throws SaException {
    String[] splits = StringUtils.split(value, ",");
    if (splits.length != 3) {
      logger.error("illegal data mapping format: {}", value);
      throw new SaException("illegal data mapping format");
    }
    String columnName = splits[0].trim();
    Integer columnIndex = Integer.parseInt(splits[1].trim());
    DataType dataType = DataType.fromString(splits[2].trim());
    if (dataType == DataType.UNKNOWN) {
      logger.error("illegal data type: {}", splits[2].trim());
      throw new SaException("illegal data type");
    }
    return new SaDataMapping(columnName, columnIndex, dataType);
  }

  String getColumnName() {
    return columnName;
  }

  public Integer getColumnIndex() {
    return columnIndex;
  }

  public DataType getDataType() {
    return dataType;
  }
}
