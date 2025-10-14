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
 * Data Types for a Context
 */
public class ContextDataTypePutApiRequest {

  private UUID id;
  private UUID idGenericDataType;
  private Integer sequence;
  private String name;
  private String description;
  private String alias;
  private String contextValue;
  private Timestamp updatedAt;
  private String updatedBy;

  public ContextDataTypePutApiRequest () {}

  @Hidden
  public Map<String, Object> getUpdateRequest() {
    
     var request = new LinkedHashMap<String, Object>();
    request.put("sql", "SELECT pareto.u_context_data_type(?,?,?,?,?,?,?,?,?)");
    request.put("id", this.id);
    request.put("idGenericDataType", this.idGenericDataType);
    request.put("sequence", this.sequence);
    request.put("name", this.name);
    request.put("description", this.description);
    request.put("alias", this.alias);
    request.put("contextValue", this.contextValue);
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
    
  public UUID getIdGenericDataType() {
    return this.idGenericDataType;
  }
    
  public UUID setIdGenericDataType(UUID idGenericDataType) {
    return this.idGenericDataType = idGenericDataType;
  }    
    
  public Integer getSequence() {
    return this.sequence;
  }
    
  public Integer setSequence(Integer sequence) {
    return this.sequence = sequence;
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
    
  public String getAlias() {
    return this.alias;
  }
    
  public String setAlias(String alias) {
    return this.alias = alias;
  }    
    
  public String getContextValue() {
    return this.contextValue;
  }
    
  public String setContextValue(String contextValue) {
    return this.contextValue = contextValue;
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