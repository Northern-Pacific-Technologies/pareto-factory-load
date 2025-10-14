package com.norpactech.pf.loader.dto;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import java.lang.Integer;
import java.lang.String;
import java.sql.Timestamp;
import java.util.Map;
import java.util.UUID;
import java.util.LinkedHashMap;
import io.swagger.v3.oas.annotations.Hidden;

/**
 * Put API Request
 * 
 * Allowed Properties for Project Components
 */
public class ProjectComponentPropertyPutApiRequest {

  private UUID id;
  private Integer sequence;
  private String dataObjectFilter;
  private String propertyFilter;
  private Timestamp updatedAt;
  private String updatedBy;

  public ProjectComponentPropertyPutApiRequest () {}

  @Hidden
  public Map<String, Object> getUpdateRequest() {
    
     var request = new LinkedHashMap<String, Object>();
    request.put("sql", "SELECT pareto.u_project_component_property(?,?,?,?,?,?)");
    request.put("id", this.id);
    request.put("sequence", this.sequence);
    request.put("dataObjectFilter", this.dataObjectFilter);
    request.put("propertyFilter", this.propertyFilter);
    request.put("updatedAt", this.updatedAt);
    request.put("updatedBy", this.updatedBy);
    return request;
  }
  
  // Getters and Setters...
    
  public UUID getId() {
    return this.id;
  }
    
  public UUID setId(UUID id) {
    return this.id = id;
  }    
    
  public Integer getSequence() {
    return this.sequence;
  }
    
  public Integer setSequence(Integer sequence) {
    return this.sequence = sequence;
  }    
    
  public String getDataObjectFilter() {
    return this.dataObjectFilter;
  }
    
  public String setDataObjectFilter(String dataObjectFilter) {
    return this.dataObjectFilter = dataObjectFilter;
  }    
    
  public String getPropertyFilter() {
    return this.propertyFilter;
  }
    
  public String setPropertyFilter(String propertyFilter) {
    return this.propertyFilter = propertyFilter;
  }    
    
  public Timestamp getUpdatedAt() {
    return this.updatedAt;
  }
    
  public Timestamp setUpdatedAt(Timestamp updatedAt) {
    return this.updatedAt = updatedAt;
  }    
    
  public String getUpdatedBy() {
    return this.updatedBy;
  }
    
  public String setUpdatedBy(String updatedBy) {
    return this.updatedBy = updatedBy;
  }    
}