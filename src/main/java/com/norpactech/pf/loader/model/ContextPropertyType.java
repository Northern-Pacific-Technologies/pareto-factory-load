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
 * API Model Class: ContextPropertyType - Context Property Types
 */
public class ContextPropertyType extends BaseModel {

  private UUID id;
  private UUID idTenant;
  private String tenantName;
  private UUID idContext;
  private String contextName;
  private UUID idGenericPropertyType;
  private String genericPropertyTypeName;
  private Integer length;
  private Integer scale;
  private Boolean isNullable;
  private String defaultValue;
  private Timestamp createdAt;
  private String createdBy;
  private Timestamp updatedAt;
  private String updatedBy;
  private Boolean isActive;

  public ContextPropertyType () {}
  public ContextPropertyType (Object obj) {
    super(obj);
  }

  public ContextPropertyType (
    UUID id,
    UUID idTenant,
    String tenantName,
    UUID idContext,
    String contextName,
    UUID idGenericPropertyType,
    String genericPropertyTypeName,
    Integer length,
    Integer scale,
    Boolean isNullable,
    String defaultValue,
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
    this.idGenericPropertyType = idGenericPropertyType;
    this.genericPropertyTypeName = genericPropertyTypeName;
    this.length = length;
    this.scale = scale;
    this.isNullable = isNullable;
    this.defaultValue = defaultValue;
    this.createdAt = createdAt;
    this.createdBy = createdBy;
    this.updatedAt = updatedAt;
    this.updatedBy = updatedBy;
    this.isActive = isActive;
  }

  public static Map<String, Object> queryRequest(Map<String, String> queryParams) throws Exception {
    
    var matchedParams = paramMatcher(queryParams, ContextPropertyType.class);
    matchedParams.put("sql", 
      "SELECT pareto.context_property_type.*, " + 
      "pareto.tenant.name as tenant_name, " + 
      "pareto.context.name as context_name, " + 
      "pareto.generic_property_type.name as generic_property_type_name " + 
      "FROM pareto.context_property_type " + 
      "JOIN pareto.tenant on (pareto.tenant.id = pareto.context_property_type.id_tenant) " + 
      "JOIN pareto.context on (pareto.context.id = pareto.context_property_type.id_context) " + 
      "JOIN pareto.generic_property_type on (pareto.generic_property_type.id = pareto.context_property_type.id_generic_property_type)");
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
    
  public UUID getIdGenericPropertyType() {
    return this.idGenericPropertyType;
  }
    
  public UUID setIdGenericPropertyType(UUID idGenericPropertyType) {
    return this.idGenericPropertyType = idGenericPropertyType;
  }    
    
  public String getGenericPropertyTypeName() {
    return this.genericPropertyTypeName;
  }
    
  public String setGenericPropertyTypeName(String genericPropertyTypeName) {
    return this.genericPropertyTypeName = genericPropertyTypeName;
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