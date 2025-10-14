package com.norpactech.pf.loader.dto;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import java.util.Map;
import java.util.UUID;
import java.util.LinkedHashMap;
import io.swagger.v3.oas.annotations.Hidden;

/**
 * Post API Request
 * 
 * Allowed Tenants for a User
 */
 public class TenantUserPostApiRequest {

  private UUID idTenant;
  private UUID idUser;

  public TenantUserPostApiRequest () {}

  @Hidden
  public Map<String, Object> getInsertRequest() {
    
    var request = new LinkedHashMap<String, Object>();
    request.put("sql", "SELECT pareto.i_tenant_user(?,?)");
    request.put("idTenant", this.idTenant);
    request.put("idUser", this.idUser);
    return request;
  }
  
  // Getters and Setters...
    
  public UUID getIdTenant() {
    return this.idTenant;
  }
    
  public UUID setIdTenant(UUID idTenant) {
    return this.idTenant = idTenant;
  }    
    
  public UUID getIdUser() {
    return this.idUser;
  }
    
  public UUID setIdUser(UUID idUser) {
    return this.idUser = idUser;
  }    
}