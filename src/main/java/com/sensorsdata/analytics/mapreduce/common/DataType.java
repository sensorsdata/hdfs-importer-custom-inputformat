package com.sensorsdata.analytics.mapreduce.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SA 支持的数据类型
 * Created by zhangguangqiang on 2017/8/9.
 */
public enum DataType {
  NUMBER(1), STRING(2), LIST(3), DATE(4), DATETIME(5), BOOL(6), UNKNOWN(-1);

  private int index;
  private static DataType[] indexArray;

  static {
    indexArray = new DataType[8];
    for (DataType dataType : DataType.values()) {
      if (dataType.index == -1) {
        indexArray[7] = dataType;
      } else {
        indexArray[dataType.index] = dataType;
      }
    }
  }

  DataType(int index) {
    this.index = index;
  }

  public static DataType fromString(String dataType) {
    try {
      return DataType.valueOf(dataType.toUpperCase());
    } catch (Exception exp) {
      logger.warn("Unknown DataType '{}'", dataType);
      return UNKNOWN;
    }
  }

  public static DataType fromInt(int index) {
    if (index >= 1 && index <= 6) {
      return indexArray[index];
    }
    return UNKNOWN;
  }

  public int getIndex() {
    return index;
  }
  private static final Logger logger = LoggerFactory.getLogger(DataType.class);
}
