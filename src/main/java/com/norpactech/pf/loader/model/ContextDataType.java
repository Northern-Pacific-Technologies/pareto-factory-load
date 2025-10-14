package com.norpactech.pf.loader.model;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import com.norpactech.nc.model.BaseModel;
import java.lang.Boolean;
import java.lang.Integer;
import java.lang.String;
import java.sql.Timestamp;
import java.util.Map;
import java.util.UUID;

/**
 * API Model Class: ContextDataType - Data Types for a Context
 */
public class ContextDataType extends BaseModel {

  private UUID id;
  private UUID idTenant;
  private String tenantName;
  private UUID idContext;
  private String contextName;
  private UUID idGenericDataType;
  private String genericDataTypeName;
  private Integer sequence;
  private String name;
  private String description;
  private String alias;
  private String contextValue;
  private Timestamp createdAt;
  private String createdBy;
  private Timestamp updatedAt;
  private String updatedBy;
  private Boolean isActive;

  public ContextDataType () {}
  public ContextDataType (Object obj) {
    super(obj);
  }

  public ContextDataType (
    UUID id,
    UUID idTenant,
    String tenantName,
    UUID idContext,
    String contextName,
    UUID idGenericDataType,
    String genericDataTypeName,
    Integer sequence,
    String name,
    String description,
    String alias,
    String contextValue,
    Timestamp createdAt,
    String createdBy,
    Timestamp updatedAt,
    String updatedBy,
    Boolean isActive)
 {
    this.id = id;
    this.idTenant = idTenant;
    this.tenantName = tenantName;
    this.idContext = idContext;
    this.contextName = contextName;
    this.idGenericDataType = idGenericDataType;
    this.genericDataTypeName = genericDataTypeName;
    this.sequence = sequence;
    this.name = name;
    this.description = description;
    this.alias = alias;
    this.contextValue = contextValue;
    this.createdAt = createdAt;
    this.createdBy = createdBy;
    this.updatedAt = updatedAt;
    this.updatedBy = updatedBy;
    this.isActive = isActive;
  }

  public static Map<String, Object> queryRequest(Map<String, String> queryParams) throws Exception {
    
    var matchedParams = paramMatcher(queryParams, ContextDataType.class);
    matchedParams.put("sql", 
      "SELECT pareto.context_data_type.*, " + 
      "pareto.tenant.name as tenant_name, " + 
      "pareto.context.name as context_name, " + 
      "pareto.generic_data_type.name as generic_data_type_name " + 
      "FROM pareto.context_data_type " + 
      "JOIN pareto.tenant on (pareto.tenant.id = pareto.context_data_type.id_tenant) " + 
      "JOIN pareto.context on (pareto.context.id = pareto.context_data_type.id_context) " + 
      "JOIN pareto.generic_data_type on (pareto.generic_data_type.id = pareto.context_data_type.id_generic_data_type)");
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
    
  public UUID getIdContext() {
    return this.idContext;
  }
    
  public UUID setIdContext(UUID idContext) {
    return this.idContext = idContext;
  }    
    
  public String getContextName() {
    return this.contextName;
  }
    
  public String setContextName(String contextName) {
    return this.contextName = contextName;
  }    
    
  public UUID getIdGenericDataType() {
    return this.idGenericDataType;
  }
    
  public UUID setIdGenericDataType(UUID idGenericDataType) {
    return this.idGenericDataType = idGenericDataType;
  }    
    
  public String getGenericDataTypeName() {
    return this.genericDataTypeName;
  }
    
  public String setGenericDataTypeName(String genericDataTypeName) {
    return this.genericDataTypeName = genericDataTypeName;
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