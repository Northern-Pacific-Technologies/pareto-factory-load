package com.norpactech.pf.loader.dto;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import java.lang.Boolean;
import java.lang.String;
import java.util.Map;
import java.util.UUID;
import java.util.LinkedHashMap;
import io.swagger.v3.oas.annotations.Hidden;

/**
 * Post API Request
 * 
 * Object
 */
 public class DataObjectPostApiRequest {

  private UUID idTenant;
  private UUID idSchema;
  private String name;
  private String description;
  private Boolean hasIdentifier;
  private Boolean hasTenancy;
  private Boolean hasAudit;
  private Boolean hasActive;
  private String createdBy;

  public DataObjectPostApiRequest () {}

  @Hidden
  public Map<String, Object> getInsertRequest() {
    
    var request = new LinkedHashMap<String, Object>();
    request.put("sql", "SELECT pareto.i_data_object(?,?,?,?,?,?,?,?,?)");
    request.put("idTenant", this.idTenant);
    request.put("idSchema", this.idSchema);
    request.put("name", this.name);
    request.put("description", this.description);
    request.put("hasIdentifier", this.hasIdentifier);
    request.put("hasTenancy", this.hasTenancy);
    request.put("hasAudit", this.hasAudit);
    request.put("hasActive", this.hasActive);
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
    
  public UUID getIdSchema() {
    return this.idSchema;
  }
    
  public UUID setIdSchema(UUID idSchema) {
    return this.idSchema = idSchema;
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
    
  public String getCreatedBy() {
    return this.createdBy;
  }
    
  public String setCreatedBy(String createdBy) {
    return this.createdBy = createdBy;
  }    
}