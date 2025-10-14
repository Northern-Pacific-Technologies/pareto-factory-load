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
import com.norpactech.pf.loader.dto.ProjectDeleteApiRequest;
import com.norpactech.pf.loader.dto.ProjectPutApiRequest;
import com.norpactech.pf.loader.dto.ProjectPostApiRequest;

import com.norpactech.pf.loader.model.Project;

public class ProjectRepository extends ParetoNativeRepository<Project> {
  
  private static final String RELATIVE_URL = "/project";

  @Override
  protected String getRelativeURL() {
    return RELATIVE_URL;
  }

  public Project get(UUID id) throws Exception {
    return super.findOne(Project.class, new HashMap<>(Map.of("id", id)));
  }

  public Project findOne(UUID idTenant, UUID idSchema, String name) throws Exception {
    return super.findOne(Project.class, new HashMap<>(Map.of("idTenant", idTenant, "idSchema", idSchema, "name", name)));
  }
  
  public List<Project> find(Map<String, Object> params) throws Exception {
    return super.find(Project.class, params);
  }
    
  public ApiResponse save(ProjectPostApiRequest request) throws Exception {
    return super.post(toParams(request));
  }  
  
  public ApiResponse save(ProjectPutApiRequest request) throws Exception {
    return super.put(toParams(request));
  } 

  public ApiResponse delete(ProjectDeleteApiRequest request) throws Exception {
    return super.delete(toParams(request));
  }
}