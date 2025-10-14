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
 * Reference Tables
 */
 public class RefTablesPostApiRequest {

  private UUID idTenant;
  private UUID idRefTableType;
  private String name;
  private String description;
  private String value;
  private Integer sequence;
  private String createdBy;

  public RefTablesPostApiRequest () {}

  @Hidden
  public Map<String, Object> getInsertRequest() {
    
    var request = new LinkedHashMap<String, Object>();
    request.put("sql", "SELECT pareto.i_ref_tables(?,?,?,?,?,?,?)");
    request.put("idTenant", this.idTenant);
    request.put("idRefTableType", this.idRefTableType);
    request.put("name", this.name);
    request.put("description", this.description);
    request.put("value", this.value);
    request.put("sequence", this.sequence);
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
    
  public UUID getIdRefTableType() {
    return this.idRefTableType;
  }
    
  public UUID setIdRefTableType(UUID idRefTableType) {
    return this.idRefTableType = idRefTableType;
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
    
  public String getValue() {
    return this.value;
  }
    
  public String setValue(String value) {
    return this.value = value;
  }    
    
  public Integer getSequence() {
    return this.sequence;
  }
    
  public Integer setSequence(Integer sequence) {
    return this.sequence = sequence;
  }    
    
  public String getCreatedBy() {
    return this.createdBy;
  }
    
  public String setCreatedBy(String createdBy) {
    return this.createdBy = createdBy;
  }    
}