package com.norpactech.pf.loader.dto;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import java.lang.String;
import java.util.Map;
import java.util.UUID;
import java.util.LinkedHashMap;
import io.swagger.v3.oas.annotations.Hidden;

/**
 * Post API Request
 * 
 * Pareto Project
 */
 public class ProjectPostApiRequest {

  private UUID idTenant;
  private UUID idSchema;
  private String name;
  private String description;
  private String domain;
  private String artifact;
  private String createdBy;

  public ProjectPostApiRequest () {}

  @Hidden
  public Map<String, Object> getInsertRequest() {
    
    var request = new LinkedHashMap<String, Object>();
    request.put("sql", "SELECT pareto.i_project(?,?,?,?,?,?,?)");
    request.put("idTenant", this.idTenant);
    request.put("idSchema", this.idSchema);
    request.put("name", this.name);
    request.put("description", this.description);
    request.put("domain", this.domain);
    request.put("artifact", this.artifact);
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
    
  public String getDomain() {
    return this.domain;
  }
    
  public String setDomain(String domain) {
    return this.domain = domain;
  }    
    
  public String getArtifact() {
    return this.artifact;
  }
    
  public String setArtifact(String artifact) {
    return this.artifact = artifact;
  }    
    
  public String getCreatedBy() {
    return this.createdBy;
  }
    
  public String setCreatedBy(String createdBy) {
    return this.createdBy = createdBy;
  }    
}