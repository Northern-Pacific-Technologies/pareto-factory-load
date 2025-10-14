package com.norpactech.pf.loader.vo;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
public class JwtRequestVO {

  private String username;
  private String password;
  
  public JwtRequestVO(String username, String password) {
    this.username = username;
    this.password = password;
  }
  
  public String getEmail() {
    return username;
  }
  public void setEmail(String username) {
    this.username = username;
  }
  public String getPassword() {
    return password;
  }
  public void setPassword(String password) {
    this.password = password;
  }  
}
