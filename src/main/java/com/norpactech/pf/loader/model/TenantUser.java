package com.norpactech.pf.loader.model;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import com.norpactech.nc.model.BaseModel;
import java.lang.String;
import java.util.Map;
import java.util.UUID;

/**
 * API Model Class: TenantUser - Allowed Tenants for a User
 */
public class TenantUser extends BaseModel {

  private UUID idTenant;
  private String tenantName;
  private UUID idUser;
  private String userEmail;

  public TenantUser () {}
  public TenantUser (Object obj) {
    super(obj);
  }

  public TenantUser (
    UUID idTenant,
    String tenantName,
    UUID idUser,
    String userEmail)
 {
    this.idTenant = idTenant;
    this.tenantName = tenantName;
    this.idUser = idUser;
    this.userEmail = userEmail;
  }

  public static Map<String, Object> queryRequest(Map<String, String> queryParams) throws Exception {
    
    var matchedParams = paramMatcher(queryParams, TenantUser.class);
    matchedParams.put("sql", 
      "SELECT pareto.tenant_user.*, " + 
      "pareto.tenant.name as tenant_name, " + 
      "pareto.user.email as user_email " + 
      "FROM pareto.tenant_user " + 
      "JOIN pareto.tenant on (pareto.tenant.id = pareto.tenant_user.id_tenant) " + 
      "JOIN pareto.user on (pareto.user.id = pareto.tenant_user.id_user)");
    return matchedParams;
  }

  // Getters and Setters...
    
  public UUID getIdTenant() {
    return this.idTenant;
  }
    
  public UUID setIdTenant(UUID idTenant) {
    return this.idTenant = idTenant;
  }    
    
  public String getTenantName() {
    return this.tenantName;
  }
    
  public String setTenantName(String tenantName) {
    return this.tenantName = tenantName;
  }    
    
  public UUID getIdUser() {
    return this.idUser;
  }
    
  public UUID setIdUser(UUID idUser) {
    return this.idUser = idUser;
  }    
    
  public String getUserEmail() {
    return this.userEmail;
  }
    
  public String setUserEmail(String userEmail) {
    return this.userEmail = userEmail;
  }    
}