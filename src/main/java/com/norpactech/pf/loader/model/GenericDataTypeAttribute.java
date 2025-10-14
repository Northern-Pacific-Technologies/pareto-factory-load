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
 * API Model Class: GenericDataTypeAttribute - Attributes for generic data types
 */
public class GenericDataTypeAttribute extends BaseModel {

  private UUID id;
  private UUID idTenant;
  private String tenantName;
  private UUID idGenericDataType;
  private String genericDataTypeName;
  private UUID idRtAttrDataType;
  private String name;
  private String description;
  private Timestamp createdAt;
  private String createdBy;
  private Timestamp updatedAt;
  private String updatedBy;
  private Boolean isActive;

  public GenericDataTypeAttribute () {}
  public GenericDataTypeAttribute (Object obj) {
    super(obj);
  }

  public GenericDataTypeAttribute (
    UUID id,
    UUID idTenant,
    String tenantName,
    UUID idGenericDataType,
    String genericDataTypeName,
    UUID idRtAttrDataType,
    String name,
    String description,
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
    this.idRtAttrDataType = idRtAttrDataType;
    this.name = name;
    this.description = description;
    this.createdAt = createdAt;
    this.createdBy = createdBy;
    this.updatedAt = updatedAt;
    this.updatedBy = updatedBy;
    this.isActive = isActive;
  }

  public static Map<String, Object> queryRequest(Map<String, String> queryParams) throws Exception {
    
    var matchedParams = paramMatcher(queryParams, GenericDataTypeAttribute.class);
    matchedParams.put("sql", 
      "SELECT pareto.generic_data_type_attribute.*, " + 
      "pareto.tenant.name as tenant_name, " + 
      "pareto.generic_data_type.name as generic_data_type_name " + 
      "FROM pareto.generic_data_type_attribute " + 
      "JOIN pareto.tenant on (pareto.tenant.id = pareto.generic_data_type_attribute.id_tenant) " + 
      "JOIN pareto.generic_data_type on (pareto.generic_data_type.id = pareto.generic_data_type_attribute.id_generic_data_type)");
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
    
  public UUID getIdRtAttrDataType() {
    return this.idRtAttrDataType;
  }
    
  public UUID setIdRtAttrDataType(UUID idRtAttrDataType) {
    return this.idRtAttrDataType = idRtAttrDataType;
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