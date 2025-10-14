package com.norpactech.pf.loader.dto;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import java.lang.String;
import java.util.Map;
import java.util.UUID;
import java.util.LinkedHashMap;
import io.swagger.v3.oas.annotations.Hidden;

/**
 * Post API Request
 * 
 * Registered Plugins
 */
 public class PluginPostApiRequest {

  private UUID idContext;
  private String name;
  private String description;
  private String pluginService;
  private String createdBy;

  public PluginPostApiRequest () {}

  @Hidden
  public Map<String, Object> getInsertRequest() {
    
    var request = new LinkedHashMap<String, Object>();
    request.put("sql", "SELECT pareto.i_plugin(?,?,?,?,?)");
    request.put("idContext", this.idContext);
    request.put("name", this.name);
    request.put("description", this.description);
    request.put("pluginService", this.pluginService);
    request.put("createdBy", this.createdBy);
    return request;
  }
  
  // Getters and Setters...
    
  public UUID getIdContext() {
    return this.idContext;
  }
    
  public UUID setIdContext(UUID idContext) {
    return this.idContext = idContext;
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
    
  public String getPluginService() {
    return this.pluginService;
  }
    
  public String setPluginService(String pluginService) {
    return this.pluginService = pluginService;
  }    
    
  public String getCreatedBy() {
    return this.createdBy;
  }
    
  public String setCreatedBy(String createdBy) {
    return this.createdBy = createdBy;
  }    
}