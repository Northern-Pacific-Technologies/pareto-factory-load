package com.norpactech.pf.config;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import java.util.UUID;

public class Globals {

  private static UUID idTenant;

  public static UUID getIdTenant() {
    return idTenant;
  }

  public static void setIdTenant(UUID idTenant) {
    Globals.idTenant = idTenant;
  }
}