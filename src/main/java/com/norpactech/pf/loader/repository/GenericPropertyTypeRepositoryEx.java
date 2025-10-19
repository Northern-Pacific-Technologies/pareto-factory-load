package com.norpactech.pf.loader.repository;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.norpactech.pf.loader.model.GenericPropertyType;

public class GenericPropertyTypeRepositoryEx extends GenericPropertyTypeRepository {

  public GenericPropertyType findOne(UUID idTenant, String name) throws Exception {
    return super.findOne(GenericPropertyType.class, new HashMap<>(Map.of("idTenant", idTenant, "name", name)));
  }  
}