package com.norpactech.pf.utils;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.google.gson.internal.LinkedTreeMap;
import com.norpactech.pf.loader.enums.EnumApiCodes;
import com.norpactech.pf.loader.enums.EnumStatus;
import com.norpactech.pf.loader.exception.ApiResponseException;
/**
 * Handles: Successful, Unsuccessful, and Exception responses
 */
public class ApiResponse {
  
  private EnumStatus status;
  private Object data;
  private Object error;
  private Meta meta;
  
  public EnumStatus getStatus() {
    return status;
  }
  
  public ApiResponse() {
    
    this.setData(null);
    this.setError(null);
    this.setMeta(new ApiSuccessMeta(EnumApiCodes.OK));
    this.status = EnumStatus.OK;    
  }
  
  public ApiResponse(Object object) {
    
    this.setData(object);
    this.setError(null);
    this.setMeta(new ApiSuccessMeta(EnumApiCodes.OK));
    this.status = EnumStatus.OK;    
  }

  public ApiResponse(PgsqlExecResponse response) {
    
    if (response.getStatus().equals(EnumStatus.OK.getName())) {
      this.setData(response.getData());
      this.setError(null);
      this.setMeta(new ApiSuccessMeta(EnumApiCodes.CREATED));
      this.status = EnumStatus.OK;
    }
    else {
      this.setData(null);
      this.setError(response.getErrors());
      this.setMeta(new ApiErrorMeta(
          EnumApiCodes.UNPROCESSABLE_ENTITY, 
          response.getErrorCode(),
          response.getMessage(),
          response.getHint(),
          response.getDetail()));
      
      this.status = EnumStatus.ERROR;    
    }
    
  }
  
  public ApiResponse(Exception e) {

    this.setData(null);
    this.setError(e.getMessage());
    this.setMeta(new ApiErrorMeta(
        EnumApiCodes.UNPROCESSABLE_ENTITY, 
        "",
        "",
        "",
        ParseUtils.stackTrace(e)));

    this.status = EnumStatus.ERROR;    
  }


  public <T> ArrayList<T> readList(Class<T> clazz) throws Exception {

    if (this.getData() == null) {
      return null;
    }
    ArrayList<?> arrayList = (ArrayList<?>) this.getData();
    ArrayList<T> resultList = new ArrayList<>();
    
    for (Object element : arrayList) {
        T convertedElement = readObject(element, clazz);
        resultList.add(convertedElement);
    }
    return resultList;
  }

  public <T> T readObject(Class<T> clazz) throws Exception {

    if (this.getData() == null) {
      return null;
    }
    return readObject(this.getData(), clazz);
  }

  public <T> T readObject(Object object, Class<T> clazz) throws Exception {
    
    if (object == null) {
      return null;
    }
    
    @SuppressWarnings("unchecked")
    LinkedTreeMap<String, Object> map = (LinkedTreeMap<String, Object>)  object;
        
    T instance = clazz.getDeclaredConstructor().newInstance();

    for (Map.Entry<String, Object> entry : map.entrySet()) {
      String fieldName = entry.getKey();
      Object fieldValue = entry.getValue();

      try {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);

        if (field.getType() == java.lang.Integer.class) {
          String value = fieldValue.toString();
          if (value.contains(".")) {
            fieldValue = Integer.valueOf(value.split("\\.")[0]);
          }
        }
        
        if (field.getType() == java.util.UUID.class && fieldValue instanceof String) {
          fieldValue = UUID.fromString((String) fieldValue);
        }
        
        if (field.getType() == Timestamp.class && fieldValue instanceof String) {
          String dateString = (String) fieldValue;
          String dateFormat = "";
          if (dateString.contains("AM") || dateString.contains("PM")) {
            dateFormat = "MMM dd, yyyy, h:mm:ss a";
          }
          else if (dateString.contains("T")) { 
            dateFormat = "yyyy-MM-dd'T'HH:mm:ss"; 
          }
          else {
            dateFormat = "yyyy-MM-dd HH:mm:ss";
          }
        
          try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
            Date parsedDate = simpleDateFormat.parse(dateString);
            fieldValue = new Timestamp(parsedDate.getTime());
          } 
          catch (IllegalArgumentException e) {
            throw new ApiResponseException(dateString + " is an unsuppored Timestamp format");
          }        
        } 
        // Handle LocalDateTime conversion from String
        else if (field.getType() == LocalDateTime.class && fieldValue instanceof String) {
          String dateString = (String) fieldValue;
          try {
            // Try parsing with ISO format
            if (dateString.contains("T")) {
              // Handle milliseconds and Z timezone if present
              if (dateString.contains(".") && dateString.endsWith("Z")) {
                fieldValue = LocalDateTime.parse(dateString, Constant.TIMESTAMP_FORMATTER);
              } else if (dateString.endsWith("Z")) {
                // If no milliseconds but has Z timezone
                fieldValue = LocalDateTime.parse(dateString.substring(0, dateString.length() - 1), 
                    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
              } else {
                // Standard ISO without Z
                fieldValue = LocalDateTime.parse(dateString, 
                    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
              }
            } else {
              // Try standard date time format
              fieldValue = LocalDateTime.parse(dateString, 
                  DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            }
          } catch (Exception e) {
            throw new ApiResponseException(dateString + " is an unsupported LocalDateTime format: " + e.getMessage());
          }
        }
        field.set(instance, fieldValue);
      } 
      catch (NoSuchFieldException e) {
        // Ignore fields that don't exist in the target class
      }
    }

    return instance;
}  
  
  @SuppressWarnings("unchecked")
  public Object getProperty(String name) {
    
    name = TextUtils.toSnakeCase(name);
    var data = (LinkedHashMap<String, Object>) getData();
    var property = data.get(name);
    
    if (property == null) {
      return null;
    }
    if (name.equalsIgnoreCase("id")) {
      var id = (String) property;
      return UUID.fromString(id);
    } 
    else if (name.equals("updated_at")) {
      String timestamp = (String) property;
      timestamp = timestamp.replace("T", " ");
      return Timestamp.valueOf(timestamp);      
    }
    else {
      return property;
    }
  }

  public UUID getId() {
    
    return (UUID) getProperty("id");
  }
  
  public Timestamp getUpdatedAt() {
    
    return (Timestamp) getProperty("updated_at");
  }
  
  public Object getData() {
    return data;
  }

  public void setData(Object data) {
    this.data = data;
  }

  public Object getError() {
    return error;
  }

  public void setError(Object error) {
    this.error = error;
  }

  public Meta getMeta() {
    return meta;
  }

  public void setMeta(Meta meta) {
    this.meta = meta;
  }
}