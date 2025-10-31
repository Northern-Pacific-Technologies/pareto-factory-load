package com.norpactech.pf.loader.repository;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.norpactech.nc.api.utils.ApiResponse;
import com.norpactech.nc.repository.ParetoNativeRepository;
import com.norpactech.pf.loader.dto.DataIndexPropertyDeleteApiRequest;
import com.norpactech.pf.loader.dto.DataIndexPropertyPostApiRequest;
import com.norpactech.pf.loader.dto.DataIndexPropertyPutApiRequest;
import com.norpactech.pf.loader.model.DataIndexProperty;

public class DataIndexPropertyRepository extends ParetoNativeRepository<DataIndexProperty> {
  
  private static final String RELATIVE_URL = "/data-index-property";

  @Override
  protected String getRelativeURL() {
    return RELATIVE_URL;
  }

  public DataIndexProperty get(UUID id) throws Exception {
    return super.findOne(DataIndexProperty.class, new HashMap<>(Map.of("id", id)));
  }

  public DataIndexProperty findOne(UUID idTenant, UUID idDataIndex, UUID idProperty) throws Exception {
    return super.findOne(DataIndexProperty.class, new HashMap<>(Map.of("idTenant", idTenant, "idDataIndex", idDataIndex, "idProperty", idProperty)));
  }
  
  public List<DataIndexProperty> find(Map<String, Object> params) throws Exception {
    return super.find(DataIndexProperty.class, params);
  }
    
  public ApiResponse save(DataIndexPropertyPostApiRequest request) throws Exception {
    return super.post(toParams(request));
  }  
  
  public ApiResponse save(DataIndexPropertyPutApiRequest request) throws Exception {
    return super.put(toParams(request));
  } 

  public ApiResponse delete(DataIndexPropertyDeleteApiRequest request) throws Exception {
    return super.delete(toParams(request));
  }
}