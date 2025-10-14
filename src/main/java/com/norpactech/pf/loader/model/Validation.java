package com.norpactech.pf.loader.model;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import com.norpactech.nc.model.BaseModel;
import java.lang.Boolean;
import java.lang.String;
import java.sql.Timestamp;
import java.util.Map;
import java.util.UUID;

/**
 * API Model Class: Validation - Validations
 */
public class Validation extends BaseModel {

  private UUID id;
  private UUID idTenant;
  private String tenantName;
  private UUID idRtValidationType;
  private String name;
  private String description;
  private String errorMsg;
  private String expression;
  private Timestamp createdAt;
  private String createdBy;
  private Timestamp updatedAt;
  private String updatedBy;
  private Boolean isActive;

  public Validation () {}
  public Validation (Object obj) {
    super(obj);
  }

  public Validation (
    UUID id,
    UUID idTenant,
    String tenantName,
    UUID idRtValidationType,
    String name,
    String description,
    String errorMsg,
    String expression,
    Timestamp createdAt,
    String createdBy,
    Timestamp updatedAt,
    String updatedBy,
    Boolean isActive)
 {
    this.id = id;
    this.idTenant = idTenant;
    this.tenantName = tenantName;
    this.idRtValidationType = idRtValidationType;
    this.name = name;
    this.description = description;
    this.errorMsg = errorMsg;
    this.expression = expression;
    this.createdAt = createdAt;
    this.createdBy = createdBy;
    this.updatedAt = updatedAt;
    this.updatedBy = updatedBy;
    this.isActive = isActive;
  }

  public static Map<String, Object> queryRequest(Map<String, String> queryParams) throws Exception {
    
    var matchedParams = paramMatcher(queryParams, Validation.class);
    matchedParams.put("sql", 
      "SELECT pareto.validation.*, " + 
      "pareto.tenant.name as tenant_name " + 
      "FROM pareto.validation " + 
      "JOIN pareto.tenant on (pareto.tenant.id = pareto.validation.id_tenant)");
    return matchedParams;
  }

  // Getters and Setters...
    
  public UUID getId() {
    return this.id;
  }
    
  public UUID setId(UUID id) {
    return this.id = id;
  }    
    
  public UUID getIdTenant() {
    return this.idTenant;
  }
    
  public UUID setIdTenant(UUID idTenant) {
    return this.idTenant = idTenant;
  }    
    
  public String getTenantName() {
    return this.tenantName;
  }
    
  public String setTenantName(String tenantName) {
    return this.tenantName = tenantName;
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
    
  public Timestamp getCreatedAt() {
    return this.createdAt;
  }
    
  public Timestamp setCreatedAt(Timestamp createdAt) {
    return this.createdAt = createdAt;
  }    
    
  public String getCreatedBy() {
    return this.createdBy;
  }
    
  public String setCreatedBy(String createdBy) {
    return this.createdBy = createdBy;
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
    
  public Boolean getIsActive() {
    return this.isActive;
  }
    
  public Boolean setIsActive(Boolean isActive) {
    return this.isActive = isActive;
  }    
}