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
 * API Model Class: DataIndexProperty - Object Index Properties
 */
public class DataIndexProperty extends BaseModel {

  private UUID id;
  private UUID idTenant;
  private String tenantName;
  private UUID idDataIndex;
  private String dataIndexName;
  private UUID idProperty;
  private String propertyName;
  private UUID idRtSortOrder;
  private Integer sequence;
  private Timestamp createdAt;
  private String createdBy;
  private Timestamp updatedAt;
  private String updatedBy;
  private Boolean isActive;

  public DataIndexProperty () {}
  public DataIndexProperty (Object obj) {
    super(obj);
  }

  public DataIndexProperty (
    UUID id,
    UUID idTenant,
    String tenantName,
    UUID idDataIndex,
    String dataIndexName,
    UUID idProperty,
    String propertyName,
    UUID idRtSortOrder,
    Integer sequence,
    Timestamp createdAt,
    String createdBy,
    Timestamp updatedAt,
    String updatedBy,
    Boolean isActive)
 {
    this.id = id;
    this.idTenant = idTenant;
    this.tenantName = tenantName;
    this.idDataIndex = idDataIndex;
    this.dataIndexName = dataIndexName;
    this.idProperty = idProperty;
    this.propertyName = propertyName;
    this.idRtSortOrder = idRtSortOrder;
    this.sequence = sequence;
    this.createdAt = createdAt;
    this.createdBy = createdBy;
    this.updatedAt = updatedAt;
    this.updatedBy = updatedBy;
    this.isActive = isActive;
  }

  public static Map<String, Object> queryRequest(Map<String, String> queryParams) throws Exception {
    
    var matchedParams = paramMatcher(queryParams, DataIndexProperty.class);
    matchedParams.put("sql", 
      "SELECT pareto.data_index_property.*, " + 
      "pareto.tenant.name as tenant_name, " + 
      "pareto.data_index.name as data_index_name, " + 
      "pareto.property.name as property_name " + 
      "FROM pareto.data_index_property " + 
      "JOIN pareto.tenant on (pareto.tenant.id = pareto.data_index_property.id_tenant) " + 
      "JOIN pareto.data_index on (pareto.data_index.id = pareto.data_index_property.id_data_index) " + 
      "JOIN pareto.property on (pareto.property.id = pareto.data_index_property.id_property)");
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
    
  public UUID getIdDataIndex() {
    return this.idDataIndex;
  }
    
  public UUID setIdDataIndex(UUID idDataIndex) {
    return this.idDataIndex = idDataIndex;
  }    
    
  public String getDataIndexName() {
    return this.dataIndexName;
  }
    
  public String setDataIndexName(String dataIndexName) {
    return this.dataIndexName = dataIndexName;
  }    
    
  public UUID getIdProperty() {
    return this.idProperty;
  }
    
  public UUID setIdProperty(UUID idProperty) {
    return this.idProperty = idProperty;
  }    
    
  public String getPropertyName() {
    return this.propertyName;
  }
    
  public String setPropertyName(String propertyName) {
    return this.propertyName = propertyName;
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