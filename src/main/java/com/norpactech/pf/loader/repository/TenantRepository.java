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
import com.norpactech.pf.loader.dto.TenantDeleteApiRequest;
import com.norpactech.pf.loader.dto.TenantPutApiRequest;
import com.norpactech.pf.loader.dto.TenantPostApiRequest;

import com.norpactech.pf.loader.model.Tenant;

public class TenantRepository extends ParetoNativeRepository<Tenant> {
  
  private static final String RELATIVE_URL = "/tenant";

  @Override
  protected String getRelativeURL() {
    return RELATIVE_URL;
  }

  public Tenant get(UUID id) throws Exception {
    return super.findOne(Tenant.class, new HashMap<>(Map.of("id", id)));
  }

  public Tenant findOne(String name) throws Exception {
    return super.findOne(Tenant.class, new HashMap<>(Map.of("name", name)));
  }
  
  public List<Tenant> find(Map<String, Object> params) throws Exception {
    return super.find(Tenant.class, params);
  }
    
  public ApiResponse save(TenantPostApiRequest request) throws Exception {
    return super.post(toParams(request));
  }  
  
  public ApiResponse save(TenantPutApiRequest request) throws Exception {
    return super.put(toParams(request));
  } 

  public ApiResponse delete(TenantDeleteApiRequest request) throws Exception {
    return super.delete(toParams(request));
  }
}