package com.norpactech.pf.loader.exception;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
public class ApiResponseException extends RuntimeException {

  private static final long serialVersionUID = -6217511074263397858L;

  public ApiResponseException(String message) {
    super(message);
  }
}
