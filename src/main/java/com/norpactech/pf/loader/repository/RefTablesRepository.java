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
import com.norpactech.pf.loader.dto.RefTablesDeleteApiRequest;
import com.norpactech.pf.loader.dto.RefTablesPostApiRequest;
import com.norpactech.pf.loader.dto.RefTablesPutApiRequest;

import com.norpactech.pf.loader.model.RefTables;

public class RefTablesRepository extends ParetoNativeRepository<RefTables> {
  
  private static final String RELATIVE_URL = "/ref-tables";

  @Override
  protected String getRelativeURL() {
    return RELATIVE_URL;
  }

  public RefTables get(UUID id) throws Exception {
    return super.findOne(RefTables.class, new HashMap<>(Map.of("id", id)));
  }

  public RefTables findOne(UUID idTenant, UUID idRefTableType, String name) throws Exception {
    return super.findOne(RefTables.class, new HashMap<>(Map.of("idTenant", idTenant, "idRefTableType", idRefTableType, "name", name)));
  }
  
  public List<RefTables> find(Map<String, Object> params) throws Exception {
    return super.find(RefTables.class, params);
  }
    
  public ApiResponse save(RefTablesPostApiRequest request) throws Exception {
    return super.post(toParams(request));
  }  
  
  public ApiResponse save(RefTablesPutApiRequest request) throws Exception {
    return super.put(toParams(request));
  } 

  public ApiResponse delete(RefTablesDeleteApiRequest request) throws Exception {
    return super.delete(toParams(request));
  }
}