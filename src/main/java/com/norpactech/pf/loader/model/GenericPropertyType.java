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
 * API Model Class: GenericPropertyType - Generic Property Types
 */
public class GenericPropertyType extends BaseModel {

  private UUID id;
  private UUID idTenant;
  private String tenantName;
  private UUID idGenericDataType;
  private String genericDataTypeName;
  private UUID idValidation;
  private String validationName;
  private String name;
  private String description;
  private Integer length;
  private Integer scale;
  private Boolean isNullable;
  private String defaultValue;
  private Timestamp createdAt;
  private String createdBy;
  private Timestamp updatedAt;
  private String updatedBy;
  private Boolean isActive;

  public GenericPropertyType () {}
  public GenericPropertyType (Object obj) {
    super(obj);
  }

  public GenericPropertyType (
    UUID id,
    UUID idTenant,
    String tenantName,
    UUID idGenericDataType,
    String genericDataTypeName,
    UUID idValidation,
    String validationName,
    String name,
    String description,
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
    this.idGenericDataType = idGenericDataType;
    this.genericDataTypeName = genericDataTypeName;
    this.idValidation = idValidation;
    this.validationName = validationName;
    this.name = name;
    this.description = description;
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
    
    var matchedParams = paramMatcher(queryParams, GenericPropertyType.class);
    matchedParams.put("sql", 
      "SELECT pareto.generic_property_type.*, " + 
      "pareto.tenant.name as tenant_name, " + 
      "pareto.generic_data_type.name as generic_data_type_name, " + 
      "pareto.validation.name as validation_name " + 
      "FROM pareto.generic_property_type " + 
      "JOIN pareto.tenant on (pareto.tenant.id = pareto.generic_property_type.id_tenant) " + 
      "JOIN pareto.generic_data_type on (pareto.generic_data_type.id = pareto.generic_property_type.id_generic_data_type) " + 
      "LEFT JOIN pareto.validation on (pareto.validation.id = pareto.generic_property_type.id_validation)");
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
    
  public UUID getIdValidation() {
    return this.idValidation;
  }
    
  public UUID setIdValidation(UUID idValidation) {
    return this.idValidation = idValidation;
  }    
    
  public String getValidationName() {
    return this.validationName;
  }
    
  public String setValidationName(String validationName) {
    return this.validationName = validationName;
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