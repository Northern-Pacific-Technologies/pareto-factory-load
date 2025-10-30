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
import com.norpactech.nc.repository.ParetoNativeRepository;
import com.norpactech.nc.api.utils.ApiResponse;
import com.norpactech.pf.loader.dto.ProjectComponentPropertyPostApiRequest;
import com.norpactech.pf.loader.dto.ProjectComponentPropertyPutApiRequest;
import com.norpactech.pf.loader.dto.ProjectComponentPropertyDeleteApiRequest;

import com.norpactech.pf.loader.model.ProjectComponentProperty;

public class ProjectComponentPropertyRepository extends ParetoNativeRepository<ProjectComponentProperty> {
  
  private static final String RELATIVE_URL = "/project-component-property";

  @Override
  protected String getRelativeURL() {
    return RELATIVE_URL;
  }

  public ProjectComponentProperty get(UUID id) throws Exception {
    return super.findOne(ProjectComponentProperty.class, new HashMap<>(Map.of("id", id)));
  }

  public ProjectComponentProperty findOne(UUID idTenant, UUID idProjectComponent, String dataObjectFilter, String propertyFilter) throws Exception {
    return super.findOne(ProjectComponentProperty.class, new HashMap<>(Map.of("idTenant", idTenant, "idProjectComponent", idProjectComponent, "dataObjectFilter", dataObjectFilter, "propertyFilter", propertyFilter)));
  }
  
  public List<ProjectComponentProperty> find(Map<String, Object> params) throws Exception {
    return super.find(ProjectComponentProperty.class, params);
  }
    
  public ApiResponse save(ProjectComponentPropertyPostApiRequest request) throws Exception {
    return super.post(toParams(request));
  }  
  
  public ApiResponse save(ProjectComponentPropertyPutApiRequest request) throws Exception {
    return super.put(toParams(request));
  } 

  public ApiResponse delete(ProjectComponentPropertyDeleteApiRequest request) throws Exception {
    return super.delete(toParams(request));
  }
}