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
import com.norpactech.pf.loader.dto.GenericPropertyTypePutApiRequest;
import com.norpactech.pf.loader.dto.GenericPropertyTypePostApiRequest;
import com.norpactech.pf.loader.dto.GenericPropertyTypeDeleteApiRequest;

import com.norpactech.pf.loader.model.GenericPropertyType;

public class GenericPropertyTypeRepository extends ParetoNativeRepository<GenericPropertyType> {
  
  private static final String RELATIVE_URL = "/generic-property-type";

  @Override
  protected String getRelativeURL() {
    return RELATIVE_URL;
  }

  public GenericPropertyType get(UUID id) throws Exception {
    return super.findOne(GenericPropertyType.class, new HashMap<>(Map.of("id", id)));
  }

  public GenericPropertyType findOne(UUID idTenant, UUID idGenericDataType, String name) throws Exception {
    return super.findOne(GenericPropertyType.class, new HashMap<>(Map.of("idTenant", idTenant, "idGenericDataType", idGenericDataType, "name", name)));
  }
  
  public GenericPropertyType findOne(UUID idTenant, String name) throws Exception {
    return super.findOne(GenericPropertyType.class, new HashMap<>(Map.of("idTenant", idTenant, "name", name)));
  }
  
  public List<GenericPropertyType> find(Map<String, Object> params) throws Exception {
    return super.find(GenericPropertyType.class, params);
  }
    
  public ApiResponse save(GenericPropertyTypePostApiRequest request) throws Exception {
    return super.post(toParams(request));
  }  
  
  public ApiResponse save(GenericPropertyTypePutApiRequest request) throws Exception {
    return super.put(toParams(request));
  } 

  public ApiResponse delete(GenericPropertyTypeDeleteApiRequest request) throws Exception {
    return super.delete(toParams(request));
  }
}