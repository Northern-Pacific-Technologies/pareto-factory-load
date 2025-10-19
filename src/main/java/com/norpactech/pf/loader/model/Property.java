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
 * API Model Class: Property - Object Properties
 */
public class Property extends BaseModel {

  private UUID id;
  private UUID idTenant;
  private String tenantName;
  private UUID idDataObject;
  private String dataObjectName;
  private UUID idGenericDataType;
  private String genericDataTypeName;
  private UUID idGenericPropertyType;
  private String genericPropertyTypeName;
  private UUID idValidation;
  private String validationName;
  private Integer sequence;
  private String name;
  private String description;
  private Boolean isUpdatable;
  private Boolean fkViewable;
  private Integer length;
  private Integer scale;
  private Boolean isNullable;
  private String defaultValue;
  private Timestamp createdAt;
  private String createdBy;
  private Timestamp updatedAt;
  private String updatedBy;
  private Boolean isActive;

  public Property () {}
  public Property (Object obj) {
    super(obj);
  }

  public Property (
    UUID id,
    UUID idTenant,
    String tenantName,
    UUID idDataObject,
    String dataObjectName,
    UUID idGenericDataType,
    String genericDataTypeName,
    UUID idGenericPropertyType,
    String genericPropertyTypeName,
    UUID idValidation,
    String validationName,
    Integer sequence,
    String name,
    String description,
    Boolean isUpdatable,
    Boolean fkViewable,
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
    this.idDataObject = idDataObject;
    this.dataObjectName = dataObjectName;
    this.idGenericDataType = idGenericDataType;
    this.genericDataTypeName = genericDataTypeName;
    this.idGenericPropertyType = idGenericPropertyType;
    this.genericPropertyTypeName = genericPropertyTypeName;
    this.idValidation = idValidation;
    this.validationName = validationName;
    this.sequence = sequence;
    this.name = name;
    this.description = description;
    this.isUpdatable = isUpdatable;
    this.fkViewable = fkViewable;
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
    
    var matchedParams = paramMatcher(queryParams, Property.class);
    matchedParams.put("sql", 
      "SELECT pareto.property.*, " + 
      "pareto.tenant.name as tenant_name, " + 
      "pareto.data_object.name as data_object_name, " + 
      "pareto.generic_data_type.name as generic_data_type_name, " + 
      "pareto.generic_property_type.name as generic_property_type_name, " + 
      "pareto.validation.name as validation_name " + 
      "FROM pareto.property " + 
      "JOIN pareto.tenant on (pareto.tenant.id = pareto.property.id_tenant) " + 
      "JOIN pareto.data_object on (pareto.data_object.id = pareto.property.id_data_object) " + 
      "JOIN pareto.generic_data_type on (pareto.generic_data_type.id = pareto.property.id_generic_data_type) " + 
      "LEFT JOIN pareto.generic_property_type on (pareto.generic_property_type.id = pareto.property.id_generic_property_type) " + 
      "LEFT JOIN pareto.validation on (pareto.validation.id = pareto.property.id_validation)");
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
    
  public UUID getIdDataObject() {
    return this.idDataObject;
  }
    
  public UUID setIdDataObject(UUID idDataObject) {
    return this.idDataObject = idDataObject;
  }    
    
  public String getDataObjectName() {
    return this.dataObjectName;
  }
    
  public String setDataObjectName(String dataObjectName) {
    return this.dataObjectName = dataObjectName;
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
    
  public Boolean getIsUpdatable() {
    return this.isUpdatable;
  }
    
  public Boolean setIsUpdatable(Boolean isUpdatable) {
    return this.isUpdatable = isUpdatable;
  }    
    
  public Boolean getFkViewable() {
    return this.fkViewable;
  }
    
  public Boolean setFkViewable(Boolean fkViewable) {
    return this.fkViewable = fkViewable;
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