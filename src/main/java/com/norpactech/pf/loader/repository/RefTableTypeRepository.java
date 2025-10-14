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
import com.norpactech.pf.loader.dto.RefTableTypePutApiRequest;
import com.norpactech.pf.loader.dto.RefTableTypePostApiRequest;
import com.norpactech.pf.loader.dto.RefTableTypeDeleteApiRequest;

import com.norpactech.pf.loader.model.RefTableType;

public class RefTableTypeRepository extends ParetoNativeRepository<RefTableType> {
  
  private static final String RELATIVE_URL = "/ref-table-type";

  @Override
  protected String getRelativeURL() {
    return RELATIVE_URL;
  }

  public RefTableType get(UUID id) throws Exception {
    return super.findOne(RefTableType.class, new HashMap<>(Map.of("id", id)));
  }

  public RefTableType findOne(UUID idTenant, String name) throws Exception {
    return super.findOne(RefTableType.class, new HashMap<>(Map.of("idTenant", idTenant, "name", name)));
  }
  
  public List<RefTableType> find(Map<String, Object> params) throws Exception {
    return super.find(RefTableType.class, params);
  }
    
  public ApiResponse save(RefTableTypePostApiRequest request) throws Exception {
    return super.post(toParams(request));
  }  
  
  public ApiResponse save(RefTableTypePutApiRequest request) throws Exception {
    return super.put(toParams(request));
  } 

  public ApiResponse delete(RefTableTypeDeleteApiRequest request) throws Exception {
    return super.delete(toParams(request));
  }
}