package com.norpactech.pf.loader.dto;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import java.lang.Integer;
import java.lang.String;
import java.util.Map;
import java.util.UUID;
import java.util.LinkedHashMap;
import io.swagger.v3.oas.annotations.Hidden;

/**
 * Post API Request
 * 
 * Allowed Properties for Project Components
 */
 public class ProjectComponentPropertyPostApiRequest {

  private UUID idTenant;
  private UUID idProjectComponent;
  private Integer sequence;
  private String dataObjectFilter;
  private String propertyFilter;
  private String createdBy;

  public ProjectComponentPropertyPostApiRequest () {}

  @Hidden
  public Map<String, Object> getInsertRequest() {
    
    var request = new LinkedHashMap<String, Object>();
    request.put("sql", "SELECT pareto.i_project_component_property(?,?,?,?,?,?)");
    request.put("idTenant", this.idTenant);
    request.put("idProjectComponent", this.idProjectComponent);
    request.put("sequence", this.sequence);
    request.put("dataObjectFilter", this.dataObjectFilter);
    request.put("propertyFilter", this.propertyFilter);
    request.put("createdBy", this.createdBy);
    return request;
  }
  
  // Getters and Setters...
    
  public UUID getIdTenant() {
    return this.idTenant;
  }
    
  public UUID setIdTenant(UUID idTenant) {
    return this.idTenant = idTenant;
  }    
    
  public UUID getIdProjectComponent() {
    return this.idProjectComponent;
  }
    
  public UUID setIdProjectComponent(UUID idProjectComponent) {
    return this.idProjectComponent = idProjectComponent;
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
    
  public String getCreatedBy() {
    return this.createdBy;
  }
    
  public String setCreatedBy(String createdBy) {
    return this.createdBy = createdBy;
  }    
}