package com.norpactech.pf.utils;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;

public class TextUtils {

  public static String escapeSql(String sql) {

    if (sql == null) {
      return null;
    }

    StringBuilder escapedText = new StringBuilder();
    for (char c : sql.toCharArray()) {
      if (c == '\'') {
        escapedText.append("''");
      } 
      else {
        escapedText.append(c);
      }
    }
    return escapedText.toString();
  }  
  
  public static UUID toUUID(String source) {

    if (StringUtils.isBlank(source)) {
      return null;
    }
    
    try {
      return UUID.fromString(source);
    } 
    catch (IllegalArgumentException e) {
      throw new RuntimeException("Invalid UUID: " + source);
    }
  }    
  
  public static String fkName(String source) {

    if (source == null) {
      return null;
    }
    
    String retVal = source.trim();
    if (source.startsWith("id_")) {
      retVal = source.substring(3);
    }
    return retVal;
  }
  
  public static String toString(String source) {

    if (StringUtils.isBlank(source)) {
      return null;
    }
    return source.trim();
  }
    
  public static Integer toInteger(String source) {

    if (StringUtils.isBlank(source)) {
      return null;
    }
    try {
      return Integer.parseInt(source);
    } 
    catch (NumberFormatException e) {
      throw new RuntimeException("Invalid integer: " + source);
    }
  }
  
  public static Boolean toBoolean(String source) {

    if (StringUtils.isBlank(source)) {
      return null;
    }

    String value = source.toLowerCase();
    if (value.equals("true")) {
      return true;
    }
    else if (value.equals("false")) {
      return false;
    }
    else {
      throw new RuntimeException("Invalid boolean: " + source);
    }
  }

  public static String toCamelCase(String snakeCase) {

    if (snakeCase == null || snakeCase.isEmpty()) {
      return snakeCase;
    }
    StringBuffer camelCase = new StringBuffer();
    boolean nextCharUpperCase = false;

    for (char c : snakeCase.toCharArray()) {
      if (c == '_') {
        nextCharUpperCase = true;
      } else {
        if (nextCharUpperCase) {
          camelCase.append(Character.toUpperCase(c));
          nextCharUpperCase = false;
        } else {
          camelCase.append(c);
        }
      }
    }
    return camelCase.toString();
  }
  
  public static String toSnakeCase(String camelCase) {

    if (camelCase == null || camelCase.isEmpty()) {
      return camelCase;
    }
    StringBuffer snakeCase = new StringBuffer();
    for (char c : camelCase.toCharArray()) {
      if (Character.isUpperCase(c)) {
        snakeCase.append('_');
        snakeCase.append(Character.toLowerCase(c));
      } else {
        snakeCase.append(c);
      }
    }
    String retVal = snakeCase.toString();
    if (retVal.startsWith("_")) {
      retVal = retVal.substring(1);
    }
    return retVal;
  }
  
  public static String toKebabCase(String camelCase) {

    if (camelCase == null || camelCase.isEmpty()) {
      return camelCase;
    }
    StringBuffer kebabCase = new StringBuffer();
    for (char c : camelCase.toCharArray()) {
      if (Character.isUpperCase(c)) {
        kebabCase.append('-');
        kebabCase.append(Character.toLowerCase(c));
      } else {
        kebabCase.append(c);
      }
    }
    String retVal = kebabCase.toString();
    if (retVal.startsWith("-")) {
      retVal = retVal.substring(1);
    }
    return retVal;
  }
  
  public static String toShortClassName(String fullClassName) {

    if (fullClassName == null || fullClassName.isEmpty()) {
      return fullClassName;
    }
    int lastDotIndex = fullClassName.lastIndexOf('.');
    if (lastDotIndex == -1) {
      return fullClassName;
    }
    return fullClassName.substring(lastDotIndex + 1);
  }
  
  public static String toGetter(String shortClassName) {

    if (shortClassName == null || shortClassName.isEmpty()) {
      return shortClassName;
    }
    return "get" + shortClassName.substring(0, 1).toUpperCase() + shortClassName.substring(1);
  }  

  public static String toSetter(String shortClassName) {

    if (shortClassName == null || shortClassName.isEmpty()) {
      return shortClassName;
    }
    return "set" + shortClassName.substring(0, 1).toUpperCase() + shortClassName.substring(1);
  }
  
  public static String toQueryString(Map<String, String> params) {

    StringBuilder queryString = new StringBuilder();
    if (params != null && !params.isEmpty()) {
      queryString.append("?");
      for (Map.Entry<String, String> entry : params.entrySet()) {
        queryString.append(entry.getKey())
        .append("=")
        .append(entry.getValue())
        .append("&");
      }
      queryString.setLength(queryString.length() - 1);
      return queryString.toString();
    }
    return "";
  }
  
  public static String lastDelimetedValue(String fullValue, String delimeter) {
    return fullValue.substring(fullValue.lastIndexOf(delimeter) + 1);
  }
}
