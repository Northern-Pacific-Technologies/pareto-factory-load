package com.norpactech.pf.loader.dto;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import java.lang.Boolean;
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
 * Context Property Types
 */
public class ContextPropertyTypePutApiRequest {

  private UUID id;
  private UUID idGenericPropertyType;
  private Integer length;
  private Integer scale;
  private Boolean isNullable;
  private String defaultValue;
  private Timestamp updatedAt;
  private String updatedBy;

  public ContextPropertyTypePutApiRequest () {}

  @Hidden
  public Map<String, Object> getUpdateRequest() {
    
     var request = new LinkedHashMap<String, Object>();
    request.put("sql", "SELECT pareto.u_context_property_type(?,?,?,?,?,?,?,?)");
    request.put("id", this.id);
    request.put("idGenericPropertyType", this.idGenericPropertyType);
    request.put("length", this.length);
    request.put("scale", this.scale);
    request.put("isNullable", this.isNullable);
    request.put("defaultValue", this.defaultValue);
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
    
  public UUID getIdGenericPropertyType() {
    return this.idGenericPropertyType;
  }
    
  public UUID setIdGenericPropertyType(UUID idGenericPropertyType) {
    return this.idGenericPropertyType = idGenericPropertyType;
  }    
    
  public Integer getLength() {
    return this.length;
  }
    
  public Integer setLength(Integer length) {
    return this.length = length;
  }    
    
  public Integer getScale() {
    return this.scale;
  }
    
  public Integer setScale(Integer scale) {
    return this.scale = scale;
  }    
    
  public Boolean getIsNullable() {
    return this.isNullable;
  }
    
  public Boolean setIsNullable(Boolean isNullable) {
    return this.isNullable = isNullable;
  }    
    
  public String getDefaultValue() {
    return this.defaultValue;
  }
    
  public String setDefaultValue(String defaultValue) {
    return this.defaultValue = defaultValue;
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