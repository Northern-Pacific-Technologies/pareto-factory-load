package com.norpactech.pf.loader.dto;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import java.lang.String;
import java.util.Map;
import java.util.LinkedHashMap;
import io.swagger.v3.oas.annotations.Hidden;

/**
 * Post API Request
 * 
 * Tenant Object
 */
 public class TenantPostApiRequest {

  private String name;
  private String description;
  private String copyright;
  private String timeZone;
  private String createdBy;

  public TenantPostApiRequest () {}

  @Hidden
  public Map<String, Object> getInsertRequest() {
    
    var request = new LinkedHashMap<String, Object>();
    request.put("sql", "SELECT pareto.i_tenant(?,?,?,?,?)");
    request.put("name", this.name);
    request.put("description", this.description);
    request.put("copyright", this.copyright);
    request.put("timeZone", this.timeZone);
    request.put("createdBy", this.createdBy);
    return request;
  }
  
  // Getters and Setters...
    
  public String getName() {
    return this.name;
  }
    
  public String setName(String name) {
    return this.name = name;
  }    
    
  public String getDescription() {
    return this.description;
  }
    
  public String setDescription(String description) {
    return this.description = description;
  }    
    
  public String getCopyright() {
    return this.copyright;
  }
    
  public String setCopyright(String copyright) {
    return this.copyright = copyright;
  }    
    
  public String getTimeZone() {
    return this.timeZone;
  }
    
  public String setTimeZone(String timeZone) {
    return this.timeZone = timeZone;
  }    
    
  public String getCreatedBy() {
    return this.createdBy;
  }
    
  public String setCreatedBy(String createdBy) {
    return this.createdBy = createdBy;
  }    
}