package com.norpactech.pf.loader.model;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import com.norpactech.nc.model.BaseModel;
import java.lang.Boolean;
import java.lang.String;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * API Model Class: User - System User
 */
public class User extends BaseModel {

  private UUID id;
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
  private Timestamp createdAt;
  private String createdBy;
  private Timestamp updatedAt;
  private String updatedBy;
  private Boolean isActive;

  public User () {}
  public User (Object obj) {
    super(obj);
  }

  public User (
    UUID id,
    String email,
    String oauthIdUser,
    String lastName,
    String firstName,
    String phone,
    String street1,
    String street2,
    String city,
    String state,
    String zipCode,
    LocalDateTime termsAccepted,
    Timestamp createdAt,
    String createdBy,
    Timestamp updatedAt,
    String updatedBy,
    Boolean isActive)
 {
    this.id = id;
    this.email = email;
    this.oauthIdUser = oauthIdUser;
    this.lastName = lastName;
    this.firstName = firstName;
    this.phone = phone;
    this.street1 = street1;
    this.street2 = street2;
    this.city = city;
    this.state = state;
    this.zipCode = zipCode;
    this.termsAccepted = termsAccepted;
    this.createdAt = createdAt;
    this.createdBy = createdBy;
    this.updatedAt = updatedAt;
    this.updatedBy = updatedBy;
    this.isActive = isActive;
  }

  public static Map<String, Object> queryRequest(Map<String, String> queryParams) throws Exception {
    
    var matchedParams = paramMatcher(queryParams, User.class);
    matchedParams.put("sql", 
      "SELECT pareto.user.* " + 
      "FROM pareto.user");
    return matchedParams;
  }

  // Getters and Setters...
    
  public UUID getId() {
    return this.id;
  }
    
  public UUID setId(UUID id) {
    return this.id = id;
  }    
    
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
    
  public Timestamp getCreatedAt() {
    return this.createdAt;
  }
    
  public Timestamp setCreatedAt(Timestamp createdAt) {
    return this.createdAt = createdAt;
  }    
    
  public String getCreatedBy() {
    return this.createdBy;
  }
    
  public String setCreatedBy(String createdBy) {
    return this.createdBy = createdBy;
  }    
    
  public Timestamp getUpdatedAt() {
    return this.updatedAt;
  }
    
  public Timestamp setUpdatedAt(Timestamp updatedAt) {
    return this.updatedAt = updatedAt;
  }    
    
  public String getUpdatedBy() {
    return this.updatedBy;
  }
    
  public String setUpdatedBy(String updatedBy) {
    return this.updatedBy = updatedBy;
  }    
    
  public Boolean getIsActive() {
    return this.isActive;
  }
    
  public Boolean setIsActive(Boolean isActive) {
    return this.isActive = isActive;
  }    
}