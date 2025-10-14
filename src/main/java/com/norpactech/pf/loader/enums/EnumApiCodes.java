package com.norpactech.pf.loader.enums;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
public enum EnumApiCodes {

  OK("200"),
  CREATED("201"),
  ACCEPTED("202"),
  NO_CONTENT("204"),
  PARTIAL_CONTENT("206"),
  UNPROCESSABLE_ENTITY("422");

  private String name;

  public static EnumApiCodes fromName(String name) {
    for (EnumApiCodes context : EnumApiCodes.values()) {
      if (context.getName().equals(name)) {
        return context;
      }
    }
    return null;
  }
  
  private EnumApiCodes(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
