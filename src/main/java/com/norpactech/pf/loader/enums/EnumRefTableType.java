package com.norpactech.pf.loader.enums;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
public enum EnumRefTableType {
  
  CARDINALITY("cardinality"),
  CARDINALITY_STRENGTH("cardinality_strength"), 
  INDEX_TYPE("index_type"),
  SORT_ORDER("sort_order"),
  VALIDATION_TYPE("validation_type"),
  TIME_ZONE("time_zone"),
  ATTR_DATA_TYPE("attr_data_type");

  private String name;

  private EnumRefTableType(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public static EnumRefTableType fromName(String name) {
    for (EnumRefTableType type : EnumRefTableType.values()) {
      if (type.getName().equals(name)) {
        return type;
      }
    }
    return null;
  }
}