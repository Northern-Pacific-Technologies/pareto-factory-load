package com.norpactech.pf.utils;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.norpactech.pf.loader.exception.ApiResponseException;

public class ParseUtils {
  
  public static String stackTrace(Exception e) {
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    e.printStackTrace(printWriter);
    return stringWriter.toString();
  }  
  
  public static List<String> parse(String input) {

    List<String> tokens = new ArrayList<>();
    StringBuilder currentToken = new StringBuilder();
    boolean insideQuotes = false;

    for (int i = 1; i < input.length() - 1; i++) { // Skip the outer parentheses
      char c = input.charAt(i);

      if (c == '"' && (i == 0 || input.charAt(i - 1) != '\\')) {
        insideQuotes = !insideQuotes;
      } 
      else if (c == ',' && !insideQuotes) {
        tokens.add(currentToken.toString().trim());
        currentToken.setLength(0); // Reset the token
        continue;
      }
      currentToken.append(c); // Append character to the current token
    }
    if (currentToken.length() > 0) {
      tokens.add(currentToken.toString().trim());
    }
    return tokens;
  }
  /**
   * Parses a string from Postgres and returns a JSON Object
   * 
   * @param input as illustrated in these comments
   * @return JSON Object
   */
  @SuppressWarnings("unchecked")
  public static Object parseJson(String input) {
    
    if (StringUtils.isEmpty(input)) {
      return "{}";
    }
    String json = input.replace("\"\"", "\"");
    json = json.replace("\"{", "{").replace("}\"", "}");
    json = json.replace("\"[", "[").replace("]\"", "]");

    String isoPattern = "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?";
    Pattern pattern = Pattern.compile(isoPattern);
    
    try {

      Map<String, Object> map = new ObjectMapper().readValue(json, Map.class);
      LinkedHashMap<String, Object> groomedMap = new LinkedHashMap<>();

      for (String key : map.keySet()) {
        String camelCaseKey = TextUtils.toCamelCase(key);
        Object value = map.get(key);
        
        if (value instanceof String) {
          if (pattern.matcher((String) value).matches()) {
            LocalDateTime dateTime = LocalDateTime.parse((String) value);
            value = dateTime.format(Constant.TIMESTAMP_FORMATTER);
          }
        }
        groomedMap.put(camelCaseKey, value);
      }
      return groomedMap;
    } 
    catch (Exception e) {
      throw new ApiResponseException("Failed to parse JSON: " + e.getMessage());
    }
  }
  
  public static String stripQuotes(String input) {
    
    if (StringUtils.isEmpty(input)) {
      return "";
    }
    if (input.startsWith("\"") && input.endsWith("\"")) {
      return input.substring(1, input.length() - 1);
    }
    return input.replace("\"\"", "\"");
  }
}