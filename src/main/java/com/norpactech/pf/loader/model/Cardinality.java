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
 * API Model Class: Cardinality - Object Cardinality
 */
public class Cardinality extends BaseModel {

  private UUID id;
  private UUID idTenant;
  private String tenantName;
  private UUID idProperty;
  private String propertyName;
  private UUID idDataObject;
  private String dataObjectName;
  private UUID idRtCardinality;
  private UUID idRtCardinalityStrength;
  private Boolean hasReferentialAction;
  private Timestamp createdAt;
  private String createdBy;
  private Timestamp updatedAt;
  private String updatedBy;
  private Boolean isActive;

  public Cardinality () {}
  public Cardinality (Object obj) {
    super(obj);
  }

  public Cardinality (
    UUID id,
    UUID idTenant,
    String tenantName,
    UUID idProperty,
    String propertyName,
    UUID idDataObject,
    String dataObjectName,
    UUID idRtCardinality,
    UUID idRtCardinalityStrength,
    Boolean hasReferentialAction,
    Timestamp createdAt,
    String createdBy,
    Timestamp updatedAt,
    String updatedBy,
    Boolean isActive)
 {
    this.id = id;
    this.idTenant = idTenant;
    this.tenantName = tenantName;
    this.idProperty = idProperty;
    this.propertyName = propertyName;
    this.idDataObject = idDataObject;
    this.dataObjectName = dataObjectName;
    this.idRtCardinality = idRtCardinality;
    this.idRtCardinalityStrength = idRtCardinalityStrength;
    this.hasReferentialAction = hasReferentialAction;
    this.createdAt = createdAt;
    this.createdBy = createdBy;
    this.updatedAt = updatedAt;
    this.updatedBy = updatedBy;
    this.isActive = isActive;
  }

  public static Map<String, Object> queryRequest(Map<String, String> queryParams) throws Exception {
    
    var matchedParams = paramMatcher(queryParams, Cardinality.class);
    matchedParams.put("sql", 
      "SELECT pareto.cardinality.*, " + 
      "pareto.tenant.name as tenant_name, " + 
      "pareto.property.name as property_name, " + 
      "pareto.data_object.name as data_object_name " + 
      "FROM pareto.cardinality " + 
      "JOIN pareto.tenant on (pareto.tenant.id = pareto.cardinality.id_tenant) " + 
      "JOIN pareto.property on (pareto.property.id = pareto.cardinality.id_property) " + 
      "JOIN pareto.data_object on (pareto.data_object.id = pareto.cardinality.id_data_object)");
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
    
  public UUID getIdRtCardinality() {
    return this.idRtCardinality;
  }
    
  public UUID setIdRtCardinality(UUID idRtCardinality) {
    return this.idRtCardinality = idRtCardinality;
  }    
    
  public UUID getIdRtCardinalityStrength() {
    return this.idRtCardinalityStrength;
  }
    
  public UUID setIdRtCardinalityStrength(UUID idRtCardinalityStrength) {
    return this.idRtCardinalityStrength = idRtCardinalityStrength;
  }    
    
  public Boolean getHasReferentialAction() {
    return this.hasReferentialAction;
  }
    
  public Boolean setHasReferentialAction(Boolean hasReferentialAction) {
    return this.hasReferentialAction = hasReferentialAction;
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