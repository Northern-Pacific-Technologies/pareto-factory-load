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
 * API Model Class: ProjectComponentProperty - Allowed Properties for Project Components
 */
public class ProjectComponentProperty extends BaseModel {

  private UUID id;
  private UUID idTenant;
  private String tenantName;
  private UUID idProjectComponent;
  private String projectComponentName;
  private Integer sequence;
  private String dataObjectFilter;
  private String propertyFilter;
  private Timestamp createdAt;
  private String createdBy;
  private Timestamp updatedAt;
  private String updatedBy;
  private Boolean isActive;

  public ProjectComponentProperty () {}
  public ProjectComponentProperty (Object obj) {
    super(obj);
  }

  public ProjectComponentProperty (
    UUID id,
    UUID idTenant,
    String tenantName,
    UUID idProjectComponent,
    String projectComponentName,
    Integer sequence,
    String dataObjectFilter,
    String propertyFilter,
    Timestamp createdAt,
    String createdBy,
    Timestamp updatedAt,
    String updatedBy,
    Boolean isActive)
 {
    this.id = id;
    this.idTenant = idTenant;
    this.tenantName = tenantName;
    this.idProjectComponent = idProjectComponent;
    this.projectComponentName = projectComponentName;
    this.sequence = sequence;
    this.dataObjectFilter = dataObjectFilter;
    this.propertyFilter = propertyFilter;
    this.createdAt = createdAt;
    this.createdBy = createdBy;
    this.updatedAt = updatedAt;
    this.updatedBy = updatedBy;
    this.isActive = isActive;
  }

  public static Map<String, Object> queryRequest(Map<String, String> queryParams) throws Exception {
    
    var matchedParams = paramMatcher(queryParams, ProjectComponentProperty.class);
    matchedParams.put("sql", 
      "SELECT pareto.project_component_property.*, " + 
      "pareto.tenant.name as tenant_name, " + 
      "pareto.project_component.name as project_component_name " + 
      "FROM pareto.project_component_property " + 
      "JOIN pareto.tenant on (pareto.tenant.id = pareto.project_component_property.id_tenant) " + 
      "JOIN pareto.project_component on (pareto.project_component.id = pareto.project_component_property.id_project_component)");
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
    
  public UUID getIdProjectComponent() {
    return this.idProjectComponent;
  }
    
  public UUID setIdProjectComponent(UUID idProjectComponent) {
    return this.idProjectComponent = idProjectComponent;
  }    
    
  public String getProjectComponentName() {
    return this.projectComponentName;
  }
    
  public String setProjectComponentName(String projectComponentName) {
    return this.projectComponentName = projectComponentName;
  }    
    
  public Integer getSequence() {
    return this.sequence;
  }
    
  public Integer setSequence(Integer sequence) {
    return this.sequence = sequence;
  }    
    
  public String getDataObjectFilter() {
    return this.dataObjectFilter;
  }
    
  public String setDataObjectFilter(String dataObjectFilter) {
    return this.dataObjectFilter = dataObjectFilter;
  }    
    
  public String getPropertyFilter() {
    return this.propertyFilter;
  }
    
  public String setPropertyFilter(String propertyFilter) {
    return this.propertyFilter = propertyFilter;
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