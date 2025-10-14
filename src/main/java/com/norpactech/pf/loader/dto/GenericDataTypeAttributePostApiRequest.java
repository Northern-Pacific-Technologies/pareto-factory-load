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
 * Attributes for generic data types
 */
 public class GenericDataTypeAttributePostApiRequest {

  private UUID idTenant;
  private UUID idGenericDataType;
  private UUID idRtAttrDataType;
  private String name;
  private String description;
  private String createdBy;

  public GenericDataTypeAttributePostApiRequest () {}

  @Hidden
  public Map<String, Object> getInsertRequest() {
    
    var request = new LinkedHashMap<String, Object>();
    request.put("sql", "SELECT pareto.i_generic_data_type_attribute(?,?,?,?,?,?)");
    request.put("idTenant", this.idTenant);
    request.put("idGenericDataType", this.idGenericDataType);
    request.put("idRtAttrDataType", this.idRtAttrDataType);
    request.put("name", this.name);
    request.put("description", this.description);
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
    
  public UUID getIdGenericDataType() {
    return this.idGenericDataType;
  }
    
  public UUID setIdGenericDataType(UUID idGenericDataType) {
    return this.idGenericDataType = idGenericDataType;
  }    
    
  public UUID getIdRtAttrDataType() {
    return this.idRtAttrDataType;
  }
    
  public UUID setIdRtAttrDataType(UUID idRtAttrDataType) {
    return this.idRtAttrDataType = idRtAttrDataType;
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
    
  public String getCreatedBy() {
    return this.createdBy;
  }
    
  public String setCreatedBy(String createdBy) {
    return this.createdBy = createdBy;
  }    
}