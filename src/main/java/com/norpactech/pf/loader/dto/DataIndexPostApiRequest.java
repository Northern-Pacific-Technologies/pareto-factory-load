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
 * Object Indexes
 */
 public class DataIndexPostApiRequest {

  private UUID idTenant;
  private UUID idDataObject;
  private UUID idRtIndexType;
  private String name;
  private String createdBy;

  public DataIndexPostApiRequest () {}

  @Hidden
  public Map<String, Object> getInsertRequest() {
    
    var request = new LinkedHashMap<String, Object>();
    request.put("sql", "SELECT pareto.i_data_index(?,?,?,?,?)");
    request.put("idTenant", this.idTenant);
    request.put("idDataObject", this.idDataObject);
    request.put("idRtIndexType", this.idRtIndexType);
    request.put("name", this.name);
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
    
  public UUID getIdDataObject() {
    return this.idDataObject;
  }
    
  public UUID setIdDataObject(UUID idDataObject) {
    return this.idDataObject = idDataObject;
  }    
    
  public UUID getIdRtIndexType() {
    return this.idRtIndexType;
  }
    
  public UUID setIdRtIndexType(UUID idRtIndexType) {
    return this.idRtIndexType = idRtIndexType;
  }    
    
  public String getName() {
    return this.name;
  }
    
  public String setName(String name) {
    return this.name = name;
  }    
    
  public String getCreatedBy() {
    return this.createdBy;
  }
    
  public String setCreatedBy(String createdBy) {
    return this.createdBy = createdBy;
  }    
}