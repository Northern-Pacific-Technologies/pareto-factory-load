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
 * Validations
 */
 public class ValidationPostApiRequest {

  private UUID idTenant;
  private UUID idRtValidationType;
  private String name;
  private String description;
  private String errorMsg;
  private String expression;
  private String createdBy;

  public ValidationPostApiRequest () {}

  @Hidden
  public Map<String, Object> getInsertRequest() {
    
    var request = new LinkedHashMap<String, Object>();
    request.put("sql", "SELECT pareto.i_validation(?,?,?,?,?,?,?)");
    request.put("idTenant", this.idTenant);
    request.put("idRtValidationType", this.idRtValidationType);
    request.put("name", this.name);
    request.put("description", this.description);
    request.put("errorMsg", this.errorMsg);
    request.put("expression", this.expression);
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
    
  public UUID getIdRtValidationType() {
    return this.idRtValidationType;
  }
    
  public UUID setIdRtValidationType(UUID idRtValidationType) {
    return this.idRtValidationType = idRtValidationType;
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
    
  public String getErrorMsg() {
    return this.errorMsg;
  }
    
  public String setErrorMsg(String errorMsg) {
    return this.errorMsg = errorMsg;
  }    
    
  public String getExpression() {
    return this.expression;
  }
    
  public String setExpression(String expression) {
    return this.expression = expression;
  }    
    
  public String getCreatedBy() {
    return this.createdBy;
  }
    
  public String setCreatedBy(String createdBy) {
    return this.createdBy = createdBy;
  }    
}