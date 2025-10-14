package com.norpactech.pf.loader.service;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norpactech.pf.loader.dto.ContextDataTypePostApiRequest;
import com.norpactech.pf.loader.dto.ContextDataTypePutApiRequest;
import com.norpactech.pf.loader.dto.UserDeleteApiRequest;
import com.norpactech.pf.utils.ApiResponse;
import com.norpactech.pf.utils.Constant;
import com.norpactech.pf.utils.TextUtils;

public class LoadContextDataType extends BaseLoader {

  private static final Logger logger = LoggerFactory.getLogger(LoadContextDataType.class);

  public LoadContextDataType(String filePath, String fileName) throws Exception {
    super(filePath, fileName);
  }
  
  public void load() throws Exception {
    
    if (!isFileAvailable()) return;

    logger.info("Beginning Context Data Type Load from: " + getFullPath());
    int persisted = 0;
    int deleted = 0;
    int errors = 0;

    try {
      for (CSVRecord csvRecord : this.getCsvParser()) {
        if (isComment(csvRecord)) {
          continue;
        }
        String action = TextUtils.toString(csvRecord.get("action")).toLowerCase();
        String tenantName = TextUtils.toString(csvRecord.get("tenant"));
        String schemaName = TextUtils.toString(csvRecord.get("schema"));
        String contextName = TextUtils.toString(csvRecord.get("context"));
        String genericName = TextUtils.toString(csvRecord.get("generic_data_type"));
        Integer sequence = TextUtils.toInteger(csvRecord.get("sequence"));
        String name = TextUtils.toString(csvRecord.get("name"));
        String description = TextUtils.toString(csvRecord.get("description"));
        String alias = TextUtils.toString(csvRecord.get("alias"));
        String contextValue = TextUtils.toString(csvRecord.get("context_value"));

        var tenant = tenantRepository.findOne(tenantName);
        if (tenant == null) {
          logger.error("Tenant {} not found. Ignoring Context Data Type {}.", tenantName, name);
          continue;
        }

        var schema = schemaRepository.findOne(tenant.getId(), schemaName);
        if (schema == null) {
          logger.error("Schema {} not found. Ignoring Context Data Type {}.", schemaName, name);
          continue;
        }        

        var context = contextRepository.findOne(contextName);
        if (context == null) {
          logger.error("Constext {} not found. Ignoring Context Data Type {}.", contextName, name);
          continue;
        }

        var genericDataType = genericDataTypeRepository.findOne(tenant.getId(), genericName);
        if (genericDataType == null) {
          logger.error("Generic Data Type {} not found. Ignoring Context Data Type {}.", genericName, name);
          continue;
        }
        var contextDataType = contextDataTypeRepository.findOne(tenant.getId(), context.getId(), genericDataType.getId());
        ApiResponse response = null; 
        
        if (action.startsWith("p")) {
          if (contextDataType == null) {
            var request = new ContextDataTypePostApiRequest();
            request.setIdTenant(tenant.getId());
            request.setIdContext(context.getId());
            request.setIdGenericDataType(genericDataType.getId());
            request.setSequence(sequence);
            request.setName(name);
            request.setDescription(description);
            request.setAlias(alias);
            request.setContextValue(contextValue);
            request.setCreatedBy(Constant.THIS_PROCESS_CREATED);
            response = contextDataTypeRepository.save(request);                    
          }
          else {
            var request = new ContextDataTypePutApiRequest();
            request.setId(contextDataType.getId());
            request.setIdGenericDataType(genericDataType.getId());
            request.setSequence(sequence);
            request.setName(name);
            request.setDescription(description);
            request.setAlias(alias);
            request.setContextValue(contextValue);
            request.setUpdatedAt(contextDataType.getUpdatedAt());
            request.setUpdatedBy(Constant.THIS_PROCESS_UPDATED);
            response = contextDataTypeRepository.save(request);
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
        else if (action.startsWith("d") && contextDataType != null) {
          var request = new UserDeleteApiRequest();
          request.setId(contextDataType.getId());
          request.setUpdatedAt(contextDataType.getUpdatedAt());
          request.setUpdatedBy(Constant.THIS_PROCESS_DELETED);
          userRepository.delete(request);
          deleted++;
        }
      }
    }
    catch (Exception e) {
      logger.error("Error Loading Context Data Type {}", e.getMessage());
      throw e;
    }
    finally {
      if (this.getCsvParser() != null) this.getCsvParser().close();
    }
    logger.info("Completed Context Data Type Load with {} persisted, {} deleted, and {} errors", persisted, deleted, errors);
  }
}