package com.norpactech.pf.loader.service;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norpactech.pf.loader.dto.GenericPropertyTypePostApiRequest;
import com.norpactech.pf.loader.dto.GenericPropertyTypePutApiRequest;
import com.norpactech.pf.loader.dto.UserDeleteApiRequest;
import com.norpactech.pf.loader.model.Validation;
import com.norpactech.pf.utils.ApiResponse;
import com.norpactech.pf.utils.Constant;
import com.norpactech.pf.utils.TextUtils;

public class LoadGenericPropertyType extends BaseLoader {

  private static final Logger logger = LoggerFactory.getLogger(LoadGenericPropertyType.class);

  public LoadGenericPropertyType(String filePath, String fileName) throws Exception {
    super(filePath, fileName);
  }
  
  public void load() throws Exception {
    
    if (!isFileAvailable()) return;

    logger.info("Beginning Generic Property Type Load from: " + getFullPath());
    int persisted = 0;
    int deleted = 0;
    int errors = 0;

    try {
      for (CSVRecord csvRecord : this.getCsvParser()) {
        if (isComment(csvRecord)) {
          continue;
        }
        var action = TextUtils.toString(csvRecord.get("action")).toLowerCase();
        var tenantName = TextUtils.toString(csvRecord.get("tenant"));
        var genericDataTypeName = TextUtils.toString(csvRecord.get("generic_data_type"));
        var name = TextUtils.toString(csvRecord.get("name"));
        var description = TextUtils.toString(csvRecord.get("description"));            
        var length = TextUtils.toInteger(csvRecord.get("length"));            
        var scale = TextUtils.toInteger(csvRecord.get("scale"));            
        var isNullable = TextUtils.toBoolean(csvRecord.get("is_nullable"));            
        var defaultValue = TextUtils.toString(csvRecord.get("default_value"));            
        var validationName = TextUtils.toString(csvRecord.get("validation"));

        var tenant = tenantRepository.findOne(tenantName);
        if (tenant == null) {
          logger.error("Tenant {} not found. Ignoring Generic Property Type {}.", tenantName, name);
          continue;
        }

        var genericDataType = genericDataTypeRepository.findOne(tenant.getId(), genericDataTypeName);
        if (genericDataType == null) {
          logger.error("Generic Data Type {} not found. Ignoring Generic Property Type {}.", genericDataTypeName, name);
          continue;
        }        

        Validation validation = null;
        if (StringUtils.isNotEmpty(validationName)) {
          validation = validationRepository.findOne(tenant.getId(), validationName);
          if (validation == null) {
            logger.error("Validation {} not found. Ignoring Generic Property Type {}.", validationName, name);
            continue;
          }        
        }        
        var genericPropertyType = genericPropertyTypeRepository.findOne(tenant.getId(), genericDataType.getId(), name);
        ApiResponse response = null; 
        
        if (action.startsWith("p")) {
          if (genericPropertyType == null) {
            var request = new GenericPropertyTypePostApiRequest();
            request.setIdTenant(tenant.getId());
            request.setIdGenericDataType(genericDataType.getId());
            request.setName(name);
            request.setDescription(description);            
            request.setLength(length);            
            request.setScale(scale);            
            request.setIsNullable(isNullable);            
            request.setDefaultValue(defaultValue);            
            request.setIdValidation(validation != null ? validation.getId() : null);            
            request.setCreatedBy(Constant.THIS_PROCESS_CREATED);
            response = genericPropertyTypeRepository.save(request);                    
          }
          else {
            var request = new GenericPropertyTypePutApiRequest();
            request.setId(genericPropertyType.getId());
            request.setName(name);
            request.setDescription(description);            
            request.setLength(length);            
            request.setScale(scale);            
            request.setIsNullable(isNullable);            
            request.setDefaultValue(defaultValue);            
            request.setIdValidation(validation != null ? validation.getId() : null);            
            request.setUpdatedAt(genericPropertyType.getUpdatedAt());
            request.setUpdatedBy(Constant.THIS_PROCESS_UPDATED);
            response = genericPropertyTypeRepository.save(request);
          }
          if (response.getData() == null) {
            if (response.getError() != null) {
              logger.error(response.getError().toString());
            }
            else {
              logger.error(this.getClass().getName() + " failed for: " + name + " " + response.getMeta().getDetail());
            }
            errors++;
          }
          else {
            persisted++;
          }          
        }
        else if (action.startsWith("d") && genericDataType != null) {
          var request = new UserDeleteApiRequest();
          request.setId(genericDataType.getId());
          request.setUpdatedAt(genericDataType.getUpdatedAt());
          request.setUpdatedBy(Constant.THIS_PROCESS_DELETED);
          userRepository.delete(request);
          deleted++;
        }
      }
    }
    catch (Exception e) {
      logger.error("Error Loading Generic Property Type {}", e.getMessage());
      throw e;
    }
    finally {
      if (this.getCsvParser() != null) this.getCsvParser().close();
    }
    logger.info("Completed Generic Property Type Load with {} persisted, {} deleted, and {} errors", persisted, deleted, errors);
  }
}