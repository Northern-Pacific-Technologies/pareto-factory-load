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
 * API Model Class: DataIndex - Object Indexes
 */
public class DataIndex extends BaseModel {

  private UUID id;
  private UUID idTenant;
  private String tenantName;
  private UUID idDataObject;
  private String dataObjectName;
  private UUID idRtIndexType;
  private String name;
  private Timestamp createdAt;
  private String createdBy;
  private Timestamp updatedAt;
  private String updatedBy;
  private Boolean isActive;

  public DataIndex () {}
  public DataIndex (Object obj) {
    super(obj);
  }

  public DataIndex (
    UUID id,
    UUID idTenant,
    String tenantName,
    UUID idDataObject,
    String dataObjectName,
    UUID idRtIndexType,
    String name,
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
    this.idRtIndexType = idRtIndexType;
    this.name = name;
    this.createdAt = createdAt;
    this.createdBy = createdBy;
    this.updatedAt = updatedAt;
    this.updatedBy = updatedBy;
    this.isActive = isActive;
  }

  public static Map<String, Object> queryRequest(Map<String, String> queryParams) throws Exception {
    
    var matchedParams = paramMatcher(queryParams, DataIndex.class);
    matchedParams.put("sql", 
      "SELECT pareto.data_index.*, " + 
      "pareto.tenant.name as tenant_name, " + 
      "pareto.data_object.name as data_object_name " + 
      "FROM pareto.data_index " + 
      "JOIN pareto.tenant on (pareto.tenant.id = pareto.data_index.id_tenant) " + 
      "JOIN pareto.data_object on (pareto.data_object.id = pareto.data_index.id_data_object)");
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
    
  public UUID getIdRtIndexType() {
    return this.idRtIndexType;
  }
    
  public UUID setIdRtIndexType(UUID idRtIndexType) {
    return this.idRtIndexType = idRtIndexType;
  }    
    
  public String getName() {
    return this.name;
  }
    
  public String setName(String name) {
    return this.name = name;
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