package com.norpactech.pf.loader.service;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norpactech.nc.utils.TextUtils;
import com.norpactech.pf.loader.dto.ContextPropertyTypePostApiRequest;
import com.norpactech.pf.loader.dto.ContextPropertyTypePutApiRequest;
import com.norpactech.pf.utils.Constant;

public class LoadContextPropertyType extends BaseLoader {

  private static final Logger logger = LoggerFactory.getLogger(LoadContextPropertyType.class);

  public LoadContextPropertyType(String filePath, String fileName) throws Exception {
    super(filePath, fileName);
  }
  
  public void load() throws Exception {
    
    if (!isFileAvailable()) return;

    logger.info("Beginning Context Property Type Load from: " + getFullPath());
    int persisted = 0;
    int deleted = 0;
    int errors = 0;

    try {
      for (CSVRecord csvRecord : this.getCsvParser()) {
        if (isComment(csvRecord)) {
          continue;
        }
        var action = TextUtils.toString(csvRecord.get("action")).toLowerCase();
        var contextName = TextUtils.toString(csvRecord.get("context"));
        var tenantName = TextUtils.toString(csvRecord.get("tenant"));
        var schemaName = TextUtils.toString(csvRecord.get("schema"));        
        var genericDataTypeName = TextUtils.toString(csvRecord.get("generic_data_type"));
        var genericPropertyTypeName = TextUtils.toString(csvRecord.get("generic_property_type"));
        var length = TextUtils.toInteger(csvRecord.get("length"));            
        var scale = TextUtils.toInteger(csvRecord.get("scale"));            
        var isNullable = TextUtils.toBoolean(csvRecord.get("is_nullable"));            
        var defaultValue = TextUtils.toString(csvRecord.get("default_value"));            

        var tenant = tenantRepository.findOne(tenantName);
        if (tenant == null) {
          logger.error("Tenant {} not found. Ignoring Context Property Type.", tenantName);
          continue;
        }

        var schema = schemaRepository.findOne(tenant.getId(), schemaName);
        if (schema == null) {
          logger.error("Schema {} not found. Ignoring Context Property Type.", schemaName);
          continue;
        }        

        var context = contextRepository.findOne(contextName);
        if (context == null) {
          logger.error("Context {} not found. Ignoring Context Property Type.", contextName);
          continue;
        }

        var genericDataType = genericDataTypeRepository.findOne(tenant.getId(), genericDataTypeName);
        if (genericDataType == null) {
          logger.error("Generic Data Type {} not found. Ignoring Context Property Type.", genericDataTypeName);
          continue;
        }
        
        var genericPropertyType = genericPropertyTypeRepository.findOne(tenant.getId(), genericDataType.getId(), genericPropertyTypeName);
        if (genericPropertyType == null) {
          logger.error("Generic Property Type {} not found. Ignoring Context Property Type.", genericPropertyTypeName);
          continue;
        }
        var contextPropertyType = contextPropertyTypeRepository.findOne(tenant.getId(), context.getId(), genericPropertyType.getId());
        
        if (action.startsWith("p")) {
          if (contextPropertyType == null) {
            var request = new ContextPropertyTypePostApiRequest();
            request.setIdTenant(tenant.getId());
            request.setIdContext(context.getId());
            request.setIdGenericPropertyType(genericPropertyType.getId());
            request.setLength(length);            
            request.setScale(scale);            
            request.setIsNullable(isNullable == null ? true : false);            
            request.setDefaultValue(defaultValue); 
            request.setCreatedBy(Constant.THIS_PROCESS_CREATED);
            var response = contextPropertyTypeRepository.save(request);   
            
            if (response.getData() == null) {
              logger.error("Context Property Type failed for: " + genericPropertyTypeName + " " + response.getMeta().getDetail());
              errors++;
            }
            else {
              persisted++;
            }
          }
          else {
            var request = new ContextPropertyTypePutApiRequest();
            request.setId(contextPropertyType.getId());
            request.setIdGenericPropertyType(genericPropertyType.getId());
            request.setLength(length);            
            request.setScale(scale);            
            request.setIsNullable(isNullable == null ? true : false);            
            request.setDefaultValue(defaultValue); 
            request.setUpdatedAt(contextPropertyType.getUpdatedAt());
            request.setUpdatedBy(Constant.THIS_PROCESS_UPDATED);
            var response = contextPropertyTypeRepository.save(request);

            if (response.getData() == null) {
              logger.error("Context Property Type failed for: " + genericPropertyTypeName + " " + response.getMeta().getDetail());
              errors++;
            }
            else {
              persisted++;
            }
          }
        }
      }
    }
    catch (Exception e) {
      logger.error("Error Loading Context Property Type {}", e.getMessage());
      throw e;
    }
    finally {
      if (this.getCsvParser() != null) this.getCsvParser().close();
    }
    logger.info("Completed Context Property Type Load with {} persisted, {} deleted, and {} errors", persisted, deleted, errors);
  }
}