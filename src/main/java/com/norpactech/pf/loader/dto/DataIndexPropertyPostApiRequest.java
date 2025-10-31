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
 * Object Index Properties
 */
 public class DataIndexPropertyPostApiRequest {

  private UUID idTenant;
  private UUID idDataIndex;
  private UUID idProperty;
  private UUID idRtSortOrder;
  private Integer sequence;
  private String createdBy;

  public DataIndexPropertyPostApiRequest () {}

  @Hidden
  public Map<String, Object> getInsertRequest() {
    
    var request = new LinkedHashMap<String, Object>();
    request.put("sql", "SELECT pareto.i_data_index_property(?,?,?,?,?,?)");
    request.put("idTenant", this.idTenant);
    request.put("idDataIndex", this.idDataIndex);
    request.put("idProperty", this.idProperty);
    request.put("idRtSortOrder", this.idRtSortOrder);
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
    
  public UUID getIdDataIndex() {
    return this.idDataIndex;
  }
    
  public UUID setIdDataIndex(UUID idDataIndex) {
    return this.idDataIndex = idDataIndex;
  }    
    
  public UUID getIdProperty() {
    return this.idProperty;
  }
    
  public UUID setIdProperty(UUID idProperty) {
    return this.idProperty = idProperty;
  }    
    
  public UUID getIdRtSortOrder() {
    return this.idRtSortOrder;
  }
    
  public UUID setIdRtSortOrder(UUID idRtSortOrder) {
    return this.idRtSortOrder = idRtSortOrder;
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