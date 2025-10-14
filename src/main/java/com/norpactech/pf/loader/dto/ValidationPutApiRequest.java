package com.norpactech.pf.loader.dto;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import java.lang.String;
import java.sql.Timestamp;
import java.util.Map;
import java.util.UUID;
import java.util.LinkedHashMap;
import io.swagger.v3.oas.annotations.Hidden;

/**
 * Put API Request
 * 
 * Validations
 */
public class ValidationPutApiRequest {

  private UUID id;
  private UUID idRtValidationType;
  private String name;
  private String description;
  private String errorMsg;
  private String expression;
  private Timestamp updatedAt;
  private String updatedBy;

  public ValidationPutApiRequest () {}

  @Hidden
  public Map<String, Object> getUpdateRequest() {
    
     var request = new LinkedHashMap<String, Object>();
    request.put("sql", "SELECT pareto.u_validation(?,?,?,?,?,?,?,?)");
    request.put("id", this.id);
    request.put("idRtValidationType", this.idRtValidationType);
    request.put("name", this.name);
    request.put("description", this.description);
    request.put("errorMsg", this.errorMsg);
    request.put("expression", this.expression);
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