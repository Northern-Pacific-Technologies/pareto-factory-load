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
 * Schema
 */
 public class SchemaPostApiRequest {

  private UUID idTenant;
  private String name;
  private String description;
  private String database;
  private String username;
  private String createdBy;

  public SchemaPostApiRequest () {}

  @Hidden
  public Map<String, Object> getInsertRequest() {
    
    var request = new LinkedHashMap<String, Object>();
    request.put("sql", "SELECT pareto.i_schema(?,?,?,?,?,?)");
    request.put("idTenant", this.idTenant);
    request.put("name", this.name);
    request.put("description", this.description);
    request.put("database", this.database);
    request.put("username", this.username);
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
    
  public String getDatabase() {
    return this.database;
  }
    
  public String setDatabase(String database) {
    return this.database = database;
  }    
    
  public String getUsername() {
    return this.username;
  }
    
  public String setUsername(String username) {
    return this.username = username;
  }    
    
  public String getCreatedBy() {
    return this.createdBy;
  }
    
  public String setCreatedBy(String createdBy) {
    return this.createdBy = createdBy;
  }    
}