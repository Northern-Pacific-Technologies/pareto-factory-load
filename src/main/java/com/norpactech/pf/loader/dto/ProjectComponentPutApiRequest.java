package com.norpactech.pf.loader.dto;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import java.lang.String;
import java.sql.Timestamp;
import java.util.Map;
import java.util.UUID;
import java.util.LinkedHashMap;
import io.swagger.v3.oas.annotations.Hidden;

/**
 * Put API Request
 * 
 * Project Component (i.e. DDL)
 */
public class ProjectComponentPutApiRequest {

  private UUID id;
  private UUID idContext;
  private UUID idPlugin;
  private String name;
  private String description;
  private String subPackage;
  private Timestamp updatedAt;
  private String updatedBy;

  public ProjectComponentPutApiRequest () {}

  @Hidden
  public Map<String, Object> getUpdateRequest() {
    
     var request = new LinkedHashMap<String, Object>();
    request.put("sql", "SELECT pareto.u_project_component(?,?,?,?,?,?,?,?)");
    request.put("id", this.id);
    request.put("idContext", this.idContext);
    request.put("idPlugin", this.idPlugin);
    request.put("name", this.name);
    request.put("description", this.description);
    request.put("subPackage", this.subPackage);
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
    
  public UUID getIdContext() {
    return this.idContext;
  }
    
  public UUID setIdContext(UUID idContext) {
    return this.idContext = idContext;
  }    
    
  public UUID getIdPlugin() {
    return this.idPlugin;
  }
    
  public UUID setIdPlugin(UUID idPlugin) {
    return this.idPlugin = idPlugin;
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
    
  public String getSubPackage() {
    return this.subPackage;
  }
    
  public String setSubPackage(String subPackage) {
    return this.subPackage = subPackage;
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