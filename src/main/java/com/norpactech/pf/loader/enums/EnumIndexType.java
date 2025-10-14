package com.norpactech.pf.loader.enums;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
public enum EnumIndexType {
  
  PRIMARY_KEY("primary_key"), 
  ALTERNATE_KEY("alt_key");
  
  private String name;

  private EnumIndexType(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public static EnumIndexType fromName(String name) {
    for (EnumIndexType type : EnumIndexType.values()) {
      if (type.getName().equals(name)) {
        return type;
      }
    }
    return null;
  }
}