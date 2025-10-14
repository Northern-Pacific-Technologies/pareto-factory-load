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
 * API Model Class: Project - Pareto Project
 */
public class Project extends BaseModel {

  private UUID id;
  private UUID idTenant;
  private String tenantName;
  private UUID idSchema;
  private String schemaName;
  private String name;
  private String description;
  private String domain;
  private String artifact;
  private Timestamp createdAt;
  private String createdBy;
  private Timestamp updatedAt;
  private String updatedBy;
  private Boolean isActive;

  public Project () {}
  public Project (Object obj) {
    super(obj);
  }

  public Project (
    UUID id,
    UUID idTenant,
    String tenantName,
    UUID idSchema,
    String schemaName,
    String name,
    String description,
    String domain,
    String artifact,
    Timestamp createdAt,
    String createdBy,
    Timestamp updatedAt,
    String updatedBy,
    Boolean isActive)
 {
    this.id = id;
    this.idTenant = idTenant;
    this.tenantName = tenantName;
    this.idSchema = idSchema;
    this.schemaName = schemaName;
    this.name = name;
    this.description = description;
    this.domain = domain;
    this.artifact = artifact;
    this.createdAt = createdAt;
    this.createdBy = createdBy;
    this.updatedAt = updatedAt;
    this.updatedBy = updatedBy;
    this.isActive = isActive;
  }

  public static Map<String, Object> queryRequest(Map<String, String> queryParams) throws Exception {
    
    var matchedParams = paramMatcher(queryParams, Project.class);
    matchedParams.put("sql", 
      "SELECT pareto.project.*, " + 
      "pareto.tenant.name as tenant_name, " + 
      "pareto.schema.name as schema_name " + 
      "FROM pareto.project " + 
      "JOIN pareto.tenant on (pareto.tenant.id = pareto.project.id_tenant) " + 
      "JOIN pareto.schema on (pareto.schema.id = pareto.project.id_schema)");
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
    
  public UUID getIdSchema() {
    return this.idSchema;
  }
    
  public UUID setIdSchema(UUID idSchema) {
    return this.idSchema = idSchema;
  }    
    
  public String getSchemaName() {
    return this.schemaName;
  }
    
  public String setSchemaName(String schemaName) {
    return this.schemaName = schemaName;
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
    
  public String getDomain() {
    return this.domain;
  }
    
  public String setDomain(String domain) {
    return this.domain = domain;
  }    
    
  public String getArtifact() {
    return this.artifact;
  }
    
  public String setArtifact(String artifact) {
    return this.artifact = artifact;
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