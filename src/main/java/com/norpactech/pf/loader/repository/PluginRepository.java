package com.norpactech.pf.loader.repository;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.UUID;
import com.norpactech.pf.utils.ApiResponse;
import com.norpactech.pf.loader.dto.PluginPostApiRequest;
import com.norpactech.pf.loader.dto.PluginPutApiRequest;
import com.norpactech.pf.loader.dto.PluginDeleteApiRequest;

import com.norpactech.pf.loader.model.Plugin;

public class PluginRepository extends ParetoNativeRepository<Plugin> {
  
  private static final String RELATIVE_URL = "/plugin";

  @Override
  protected String getRelativeURL() {
    return RELATIVE_URL;
  }

  public Plugin get(UUID id) throws Exception {
    return super.findOne(Plugin.class, new HashMap<>(Map.of("id", id)));
  }

  public Plugin findOne(UUID idContext, String name) throws Exception {
    return super.findOne(Plugin.class, new HashMap<>(Map.of("idContext", idContext, "name", name)));
  }
  
  public List<Plugin> find(Map<String, Object> params) throws Exception {
    return super.find(Plugin.class, params);
  }
    
  public ApiResponse save(PluginPostApiRequest request) throws Exception {
    return super.post(toParams(request));
  }  
  
  public ApiResponse save(PluginPutApiRequest request) throws Exception {
    return super.put(toParams(request));
  } 

  public ApiResponse delete(PluginDeleteApiRequest request) throws Exception {
    return super.delete(toParams(request));
  }
}