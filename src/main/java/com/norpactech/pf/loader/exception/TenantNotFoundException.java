package com.norpactech.pf.loader.exception;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
public class TenantNotFoundException extends Exception {

  private static final long serialVersionUID = 8741513207026307347L;

  public TenantNotFoundException(String tenant) {
    super("Tenant <" + tenant + "> not found.");
  }
}
