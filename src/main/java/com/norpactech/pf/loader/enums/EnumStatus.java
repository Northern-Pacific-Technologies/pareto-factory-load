package com.norpactech.pf.loader.enums;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
public enum EnumStatus {
  
  OK("OK"), 
  ERROR("ERROR"); 

  private String name;

  public static EnumStatus fromName(String name) {
    for (EnumStatus context : EnumStatus.values()) {
      if (context.getName().equals(name)) {
        return context;
      }
    }
    return null;
  }
  
  private EnumStatus(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
