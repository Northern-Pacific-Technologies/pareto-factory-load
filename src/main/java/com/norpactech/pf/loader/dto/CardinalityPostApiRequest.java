package com.norpactech.pf.loader.dto;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import java.lang.Boolean;
import java.lang.String;
import java.util.Map;
import java.util.UUID;
import java.util.LinkedHashMap;
import io.swagger.v3.oas.annotations.Hidden;

/**
 * Post API Request
 * 
 * Object Cardinality
 */
 public class CardinalityPostApiRequest {

  private UUID idTenant;
  private UUID idProperty;
  private UUID idDataObject;
  private UUID idRtCardinality;
  private UUID idRtCardinalityStrength;
  private Boolean hasReferentialAction;
  private String createdBy;

  public CardinalityPostApiRequest () {}

  @Hidden
  public Map<String, Object> getInsertRequest() {
    
    var request = new LinkedHashMap<String, Object>();
    request.put("sql", "SELECT pareto.i_cardinality(?,?,?,?,?,?,?)");
    request.put("idTenant", this.idTenant);
    request.put("idProperty", this.idProperty);
    request.put("idDataObject", this.idDataObject);
    request.put("idRtCardinality", this.idRtCardinality);
    request.put("idRtCardinalityStrength", this.idRtCardinalityStrength);
    request.put("hasReferentialAction", this.hasReferentialAction);
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
    
  public UUID getIdProperty() {
    return this.idProperty;
  }
    
  public UUID setIdProperty(UUID idProperty) {
    return this.idProperty = idProperty;
  }    
    
  public UUID getIdDataObject() {
    return this.idDataObject;
  }
    
  public UUID setIdDataObject(UUID idDataObject) {
    return this.idDataObject = idDataObject;
  }    
    
  public UUID getIdRtCardinality() {
    return this.idRtCardinality;
  }
    
  public UUID setIdRtCardinality(UUID idRtCardinality) {
    return this.idRtCardinality = idRtCardinality;
  }    
    
  public UUID getIdRtCardinalityStrength() {
    return this.idRtCardinalityStrength;
  }
    
  public UUID setIdRtCardinalityStrength(UUID idRtCardinalityStrength) {
    return this.idRtCardinalityStrength = idRtCardinalityStrength;
  }    
    
  public Boolean getHasReferentialAction() {
    return this.hasReferentialAction;
  }
    
  public Boolean setHasReferentialAction(Boolean hasReferentialAction) {
    return this.hasReferentialAction = hasReferentialAction;
  }    
    
  public String getCreatedBy() {
    return this.createdBy;
  }
    
  public String setCreatedBy(String createdBy) {
    return this.createdBy = createdBy;
  }    
}