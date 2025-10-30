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
import com.norpactech.pf.loader.dto.ContextPropertyTypePostApiRequest;
import com.norpactech.pf.loader.dto.ContextPropertyTypePutApiRequest;
import com.norpactech.pf.loader.dto.ContextPropertyTypeDeleteApiRequest;

import com.norpactech.pf.loader.model.ContextPropertyType;

public class ContextPropertyTypeRepository extends ParetoNativeRepository<ContextPropertyType> {
  
  private static final String RELATIVE_URL = "/context-property-type";

  @Override
  protected String getRelativeURL() {
    return RELATIVE_URL;
  }

  public ContextPropertyType get(UUID id) throws Exception {
    return super.findOne(ContextPropertyType.class, new HashMap<>(Map.of("id", id)));
  }

  public ContextPropertyType findOne(UUID idTenant, UUID idContext, UUID idGenericPropertyType) throws Exception {
    return super.findOne(ContextPropertyType.class, new HashMap<>(Map.of("idTenant", idTenant, "idContext", idContext, "idGenericPropertyType", idGenericPropertyType)));
  }
  
  public List<ContextPropertyType> find(Map<String, Object> params) throws Exception {
    return super.find(ContextPropertyType.class, params);
  }
    
  public ApiResponse save(ContextPropertyTypePostApiRequest request) throws Exception {
    return super.post(toParams(request));
  }  
  
  public ApiResponse save(ContextPropertyTypePutApiRequest request) throws Exception {
    return super.put(toParams(request));
  } 

  public ApiResponse delete(ContextPropertyTypeDeleteApiRequest request) throws Exception {
    return super.delete(toParams(request));
  }
}