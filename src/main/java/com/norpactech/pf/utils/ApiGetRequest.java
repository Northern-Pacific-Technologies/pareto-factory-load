package com.norpactech.pf.utils;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import java.util.HashMap;
import java.util.Map;
/**
 * Send a Get Request
 */
public class ApiGetRequest {

  Class<?> clazz;
  String uri;
  Map<String, String> params = new HashMap<String, String>();
  
  public ApiGetRequest(Class<?> clazz, String uri, Map<String, String> params) {
    
    this.uri = uri;
    if (params != null) {
      this.params = params;
    }
  }

  public Class<?> getClazz() {
    return clazz;
  }

  public void setClazz(Class<?> clazz) {
    this.clazz = clazz;
  }

  public String getUri() {
    return uri;
  }

  public void setUri(String uri) {
    this.uri = uri;
  }

  public Map<String, String> getParams() {
    return params;
  }

  public void setParams(Map<String, String> params) {
    this.params = params;
  }
}
