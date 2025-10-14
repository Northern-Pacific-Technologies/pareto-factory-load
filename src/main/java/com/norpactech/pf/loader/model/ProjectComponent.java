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
 * API Model Class: ProjectComponent - Project Component (i.e. DDL)
 */
public class ProjectComponent extends BaseModel {

  private UUID id;
  private UUID idTenant;
  private String tenantName;
  private UUID idProject;
  private String projectName;
  private UUID idContext;
  private String contextName;
  private UUID idPlugin;
  private String pluginName;
  private String name;
  private String description;
  private String subPackage;
  private Timestamp createdAt;
  private String createdBy;
  private Timestamp updatedAt;
  private String updatedBy;
  private Boolean isActive;

  public ProjectComponent () {}
  public ProjectComponent (Object obj) {
    super(obj);
  }

  public ProjectComponent (
    UUID id,
    UUID idTenant,
    String tenantName,
    UUID idProject,
    String projectName,
    UUID idContext,
    String contextName,
    UUID idPlugin,
    String pluginName,
    String name,
    String description,
    String subPackage,
    Timestamp createdAt,
    String createdBy,
    Timestamp updatedAt,
    String updatedBy,
    Boolean isActive)
 {
    this.id = id;
    this.idTenant = idTenant;
    this.tenantName = tenantName;
    this.idProject = idProject;
    this.projectName = projectName;
    this.idContext = idContext;
    this.contextName = contextName;
    this.idPlugin = idPlugin;
    this.pluginName = pluginName;
    this.name = name;
    this.description = description;
    this.subPackage = subPackage;
    this.createdAt = createdAt;
    this.createdBy = createdBy;
    this.updatedAt = updatedAt;
    this.updatedBy = updatedBy;
    this.isActive = isActive;
  }

  public static Map<String, Object> queryRequest(Map<String, String> queryParams) throws Exception {
    
    var matchedParams = paramMatcher(queryParams, ProjectComponent.class);
    matchedParams.put("sql", 
      "SELECT pareto.project_component.*, " + 
      "pareto.tenant.name as tenant_name, " + 
      "pareto.project.name as project_name, " + 
      "pareto.context.name as context_name, " + 
      "pareto.plugin.name as plugin_name " + 
      "FROM pareto.project_component " + 
      "JOIN pareto.tenant on (pareto.tenant.id = pareto.project_component.id_tenant) " + 
      "JOIN pareto.project on (pareto.project.id = pareto.project_component.id_project) " + 
      "JOIN pareto.context on (pareto.context.id = pareto.project_component.id_context) " + 
      "JOIN pareto.plugin on (pareto.plugin.id = pareto.project_component.id_plugin)");
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
    
  public UUID getIdProject() {
    return this.idProject;
  }
    
  public UUID setIdProject(UUID idProject) {
    return this.idProject = idProject;
  }    
    
  public String getProjectName() {
    return this.projectName;
  }
    
  public String setProjectName(String projectName) {
    return this.projectName = projectName;
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
    
  public UUID getIdPlugin() {
    return this.idPlugin;
  }
    
  public UUID setIdPlugin(UUID idPlugin) {
    return this.idPlugin = idPlugin;
  }    
    
  public String getPluginName() {
    return this.pluginName;
  }
    
  public String setPluginName(String pluginName) {
    return this.pluginName = pluginName;
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
    
  public String getSubPackage() {
    return this.subPackage;
  }
    
  public String setSubPackage(String subPackage) {
    return this.subPackage = subPackage;
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