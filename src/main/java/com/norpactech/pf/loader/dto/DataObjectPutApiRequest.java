package com.norpactech.pf.loader.dto;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import java.lang.Boolean;
import java.lang.String;
import java.sql.Timestamp;
import java.util.Map;
import java.util.UUID;
import java.util.LinkedHashMap;
import io.swagger.v3.oas.annotations.Hidden;

/**
 * Put API Request
 * 
 * Object
 */
public class DataObjectPutApiRequest {

  private UUID id;
  private String name;
  private String description;
  private Boolean hasIdentifier;
  private Boolean hasTenancy;
  private Boolean hasAudit;
  private Boolean hasActive;
  private Timestamp updatedAt;
  private String updatedBy;

  public DataObjectPutApiRequest () {}

  @Hidden
  public Map<String, Object> getUpdateRequest() {
    
     var request = new LinkedHashMap<String, Object>();
    request.put("sql", "SELECT pareto.u_data_object(?,?,?,?,?,?,?,?,?)");
    request.put("id", this.id);
    request.put("name", this.name);
    request.put("description", this.description);
    request.put("hasIdentifier", this.hasIdentifier);
    request.put("hasTenancy", this.hasTenancy);
    request.put("hasAudit", this.hasAudit);
    request.put("hasActive", this.hasActive);
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
    
  public Boolean getHasIdentifier() {
    return this.hasIdentifier;
  }
    
  public Boolean setHasIdentifier(Boolean hasIdentifier) {
    return this.hasIdentifier = hasIdentifier;
  }    
    
  public Boolean getHasTenancy() {
    return this.hasTenancy;
  }
    
  public Boolean setHasTenancy(Boolean hasTenancy) {
    return this.hasTenancy = hasTenancy;
  }    
    
  public Boolean getHasAudit() {
    return this.hasAudit;
  }
    
  public Boolean setHasAudit(Boolean hasAudit) {
    return this.hasAudit = hasAudit;
  }    
    
  public Boolean getHasActive() {
    return this.hasActive;
  }
    
  public Boolean setHasActive(Boolean hasActive) {
    return this.hasActive = hasActive;
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