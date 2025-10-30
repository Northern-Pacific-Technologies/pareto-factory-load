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
import com.norpactech.pf.loader.dto.TenantUserDeleteApiRequest;
import com.norpactech.pf.loader.dto.TenantUserPostApiRequest;

import com.norpactech.pf.loader.model.TenantUser;

public class TenantUserRepository extends ParetoNativeRepository<TenantUser> {
  
  private static final String RELATIVE_URL = "/tenant-user";

  @Override
  protected String getRelativeURL() {
    return RELATIVE_URL;
  }

  public TenantUser get(UUID idTenant, UUID idUser) throws Exception {
    return super.findOne(TenantUser.class, new HashMap<>(Map.of("idTenant", idTenant, "idUser", idUser)));
  }
  
  public List<TenantUser> find(Map<String, Object> params) throws Exception {
    return super.find(TenantUser.class, params);
  }
    
  public ApiResponse save(TenantUserPostApiRequest request) throws Exception {
    return super.post(toParams(request));
  }  

  public ApiResponse delete(TenantUserDeleteApiRequest request) throws Exception {
    return super.delete(toParams(request));
  }
}