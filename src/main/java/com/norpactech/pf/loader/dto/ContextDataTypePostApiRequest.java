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
 * Data Types for a Context
 */
 public class ContextDataTypePostApiRequest {

  private UUID idTenant;
  private UUID idContext;
  private UUID idGenericDataType;
  private Integer sequence;
  private String name;
  private String description;
  private String alias;
  private String contextValue;
  private String createdBy;

  public ContextDataTypePostApiRequest () {}

  @Hidden
  public Map<String, Object> getInsertRequest() {
    
    var request = new LinkedHashMap<String, Object>();
    request.put("sql", "SELECT pareto.i_context_data_type(?,?,?,?,?,?,?,?,?)");
    request.put("idTenant", this.idTenant);
    request.put("idContext", this.idContext);
    request.put("idGenericDataType", this.idGenericDataType);
    request.put("sequence", this.sequence);
    request.put("name", this.name);
    request.put("description", this.description);
    request.put("alias", this.alias);
    request.put("contextValue", this.contextValue);
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
    
  public UUID getIdContext() {
    return this.idContext;
  }
    
  public UUID setIdContext(UUID idContext) {
    return this.idContext = idContext;
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
    
  public String getCreatedBy() {
    return this.createdBy;
  }
    
  public String setCreatedBy(String createdBy) {
    return this.createdBy = createdBy;
  }    
}