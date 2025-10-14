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
import com.norpactech.pf.loader.dto.GenericDataTypePutApiRequest;
import com.norpactech.pf.loader.dto.GenericDataTypePostApiRequest;
import com.norpactech.pf.loader.dto.GenericDataTypeDeleteApiRequest;

import com.norpactech.pf.loader.model.GenericDataType;

public class GenericDataTypeRepository extends ParetoNativeRepository<GenericDataType> {
  
  private static final String RELATIVE_URL = "/generic-data-type";

  @Override
  protected String getRelativeURL() {
    return RELATIVE_URL;
  }

  public GenericDataType get(UUID id) throws Exception {
    return super.findOne(GenericDataType.class, new HashMap<>(Map.of("id", id)));
  }

  public GenericDataType findOne(UUID idTenant, String name) throws Exception {
    return super.findOne(GenericDataType.class, new HashMap<>(Map.of("idTenant", idTenant, "name", name)));
  }
  
  public List<GenericDataType> find(Map<String, Object> params) throws Exception {
    return super.find(GenericDataType.class, params);
  }
    
  public ApiResponse save(GenericDataTypePostApiRequest request) throws Exception {
    return super.post(toParams(request));
  }  
  
  public ApiResponse save(GenericDataTypePutApiRequest request) throws Exception {
    return super.put(toParams(request));
  } 

  public ApiResponse delete(GenericDataTypeDeleteApiRequest request) throws Exception {
    return super.delete(toParams(request));
  }
}