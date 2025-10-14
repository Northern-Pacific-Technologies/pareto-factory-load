package com.norpactech.pf.utils;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.norpactech.pf.loader.enums.EnumStatus;
import com.norpactech.pf.loader.exception.ApiResponseException;

public class PgsqlExecResponse {

  private String status;
  private Object data;     
  private Object errors;   
  private String errorCode;
  private String message;
  private String hint; 
  private String detail;
  
  public PgsqlExecResponse() {}   
  
  public PgsqlExecResponse(ResultSet rs) {   
    
    String input = null;
    try {
      input = rs.getObject(1).toString();
    }
    catch (SQLException e) {
      throw new ApiResponseException("Failed to extract the Global Database Response: " + e.getMessage());
    }
    List<String> parsed = ParseUtils.parse(input);
    
    this.status = ParseUtils.stripQuotes(parsed.get(0));
    if (this.status.equalsIgnoreCase(EnumStatus.OK.getName())) {
      this.data = ParseUtils.parseJson(parsed.get(1));
      this.errors = null;
    }
    else {
      this.data = null;
      String errors = ParseUtils.stripQuotes(parsed.get(2)).replace("\"\"", "\"");
      this.errors = new Gson().fromJson(errors, JsonArray.class);
    }
    this.errorCode = ParseUtils.stripQuotes(parsed.get(3));
    this.message = ParseUtils.stripQuotes(parsed.get(4));
    this.hint = ParseUtils.stripQuotes(parsed.get(5));
  }
  
  public String getStatus() {
    return status;
  }
  public void setStatus(String status) {
    this.status = status;
  }
  public Object getData() {
    return data;
  }
  public void setData(Object data) {
    this.data = data;
  }
  public Object getErrors() {
    return errors;
  }
  public void setErrors(Object errors) {
    this.errors = errors;
  }
  public String getErrorCode() {
    return errorCode;
  }
  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }
  public String getMessage() {
    return message;
  }
  public void setMessage(String message) {
    this.message = message;
  }
  public String getHint() {
    return hint;
  }
  public void setHint(String hint) {
    this.hint = hint;
  }
  public String getDetail() {
    return detail;
  }
  public void setDetail(String detail) {
    this.detail = detail;
  }
}