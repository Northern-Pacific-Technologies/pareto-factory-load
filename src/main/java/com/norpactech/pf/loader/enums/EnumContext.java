package com.norpactech.pf.loader.enums;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
public enum EnumContext {
  
  MSDOS("msdos"), 
  JAVA("java"), 
  TYPESCRIPT("typescript"), 
  PGSQL("pgsql"), 
  MYSQL("mysql");

  private String name;

  public static EnumContext fromName(String name) {
    for (EnumContext context : EnumContext.values()) {
      if (context.getName().equals(name)) {
        return context;
      }
    }
    return null;
  }
  
  private EnumContext(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}