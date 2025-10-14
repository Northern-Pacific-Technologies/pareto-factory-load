package com.norpactech.pf.loader.dto;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import java.lang.String;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.LinkedHashMap;
import io.swagger.v3.oas.annotations.Hidden;

/**
 * Post API Request
 * 
 * System User
 */
 public class UserPostApiRequest {

  private String email;
  private String oauthIdUser;
  private String lastName;
  private String firstName;
  private String phone;
  private String street1;
  private String street2;
  private String city;
  private String state;
  private String zipCode;
  private LocalDateTime termsAccepted;
  private String createdBy;

  public UserPostApiRequest () {}

  @Hidden
  public Map<String, Object> getInsertRequest() {
    
    var request = new LinkedHashMap<String, Object>();
    request.put("sql", "SELECT pareto.i_user(?,?,?,?,?,?,?,?,?,?,?,?)");
    request.put("email", this.email);
    request.put("oauthIdUser", this.oauthIdUser);
    request.put("lastName", this.lastName);
    request.put("firstName", this.firstName);
    request.put("phone", this.phone);
    request.put("street1", this.street1);
    request.put("street2", this.street2);
    request.put("city", this.city);
    request.put("state", this.state);
    request.put("zipCode", this.zipCode);
    request.put("termsAccepted", this.termsAccepted);
    request.put("createdBy", this.createdBy);
    return request;
  }
  
  // Getters and Setters...
    
  public String getEmail() {
    return this.email;
  }
    
  public String setEmail(String email) {
    return this.email = email;
  }    
    
  public String getOauthIdUser() {
    return this.oauthIdUser;
  }
    
  public String setOauthIdUser(String oauthIdUser) {
    return this.oauthIdUser = oauthIdUser;
  }    
    
  public String getLastName() {
    return this.lastName;
  }
    
  public String setLastName(String lastName) {
    return this.lastName = lastName;
  }    
    
  public String getFirstName() {
    return this.firstName;
  }
    
  public String setFirstName(String firstName) {
    return this.firstName = firstName;
  }    
    
  public String getPhone() {
    return this.phone;
  }
    
  public String setPhone(String phone) {
    return this.phone = phone;
  }    
    
  public String getStreet1() {
    return this.street1;
  }
    
  public String setStreet1(String street1) {
    return this.street1 = street1;
  }    
    
  public String getStreet2() {
    return this.street2;
  }
    
  public String setStreet2(String street2) {
    return this.street2 = street2;
  }    
    
  public String getCity() {
    return this.city;
  }
    
  public String setCity(String city) {
    return this.city = city;
  }    
    
  public String getState() {
    return this.state;
  }
    
  public String setState(String state) {
    return this.state = state;
  }    
    
  public String getZipCode() {
    return this.zipCode;
  }
    
  public String setZipCode(String zipCode) {
    return this.zipCode = zipCode;
  }    
    
  public LocalDateTime getTermsAccepted() {
    return this.termsAccepted;
  }
    
  public LocalDateTime setTermsAccepted(LocalDateTime termsAccepted) {
    return this.termsAccepted = termsAccepted;
  }    
    
  public String getCreatedBy() {
    return this.createdBy;
  }
    
  public String setCreatedBy(String createdBy) {
    return this.createdBy = createdBy;
  }    
}