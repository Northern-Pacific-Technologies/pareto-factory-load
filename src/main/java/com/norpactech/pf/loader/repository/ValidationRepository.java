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
import com.norpactech.pf.loader.dto.ValidationDeleteApiRequest;
import com.norpactech.pf.loader.dto.ValidationPutApiRequest;
import com.norpactech.pf.loader.dto.ValidationPostApiRequest;

import com.norpactech.pf.loader.model.Validation;

public class ValidationRepository extends ParetoNativeRepository<Validation> {
  
  private static final String RELATIVE_URL = "/validation";

  @Override
  protected String getRelativeURL() {
    return RELATIVE_URL;
  }

  public Validation get(UUID id) throws Exception {
    return super.findOne(Validation.class, new HashMap<>(Map.of("id", id)));
  }

  public Validation findOne(UUID idTenant, String name) throws Exception {
    return super.findOne(Validation.class, new HashMap<>(Map.of("idTenant", idTenant, "name", name)));
  }
  
  public List<Validation> find(Map<String, Object> params) throws Exception {
    return super.find(Validation.class, params);
  }
    
  public ApiResponse save(ValidationPostApiRequest request) throws Exception {
    return super.post(toParams(request));
  }  
  
  public ApiResponse save(ValidationPutApiRequest request) throws Exception {
    return super.put(toParams(request));
  } 

  public ApiResponse delete(ValidationDeleteApiRequest request) throws Exception {
    return super.delete(toParams(request));
  }
}