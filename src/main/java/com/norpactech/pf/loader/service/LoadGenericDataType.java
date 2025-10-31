package com.norpactech.pf.loader.service;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norpactech.nc.utils.TextUtils;
import com.norpactech.pf.loader.dto.GenericDataTypePostApiRequest;
import com.norpactech.pf.loader.dto.GenericDataTypePutApiRequest;
import com.norpactech.pf.utils.Constant;

public class LoadGenericDataType extends BaseLoader {

  private static final Logger logger = LoggerFactory.getLogger(LoadGenericDataType.class);

  public LoadGenericDataType(String filePath, String fileName) throws Exception {
    super(filePath, fileName);
  }
  
  public void load() throws Exception {
    
    if (!isFileAvailable()) return;

    logger.info("Beginning Generic Data Type Load from: " + getFullPath());
    int persisted = 0;
    int deleted = 0;
    int errors = 0;

    try {
      for (CSVRecord csvRecord : this.getCsvParser()) {
        if (isComment(csvRecord)) {
          continue;
        }
        var action = csvRecord.get("action").toLowerCase();
        var tenantName = TextUtils.toString(csvRecord.get("tenant"));
        var sequence = TextUtils.toInteger(csvRecord.get("sequence"));
        var name = TextUtils.toString(csvRecord.get("name"));
        var description = TextUtils.toString(csvRecord.get("description"));
        var alias = TextUtils.toString(csvRecord.get("alias"));

        var tenant = tenantRepository.findOne(tenantName);
        if (tenant == null) {
          logger.error("Tenant {} not found. Ignoring Generic Data Type {}.", tenantName, name);
          continue;
        }
        
        var genericDataType = genericDataTypeRepository.findOne(tenant.getId(), name);
        
        if (action.startsWith("p")) {
          if (genericDataType == null) {
            var request = new GenericDataTypePostApiRequest();
            request.setIdTenant(tenant.getId());
            request.setSequence(sequence);
            request.setName(name);
            request.setDescription(description);
            request.setAlias(alias);
            request.setCreatedBy(Constant.THIS_PROCESS_CREATED);
            var response = genericDataTypeRepository.save(request);  
            
            if (response.getData() == null) {
              logger.error("Generic Data Type failed for: {}, {}", name, response.getMeta().getDetail());
              errors++;
            }
            else {
              persisted++;
            }
          }
          else {
            var request = new GenericDataTypePutApiRequest();
            request.setId(genericDataType.getId());
            request.setSequence(sequence);
            request.setName(name);
            request.setDescription(description);
            request.setAlias(alias);
            request.setUpdatedAt(genericDataType.getUpdatedAt());
            request.setUpdatedBy(Constant.THIS_PROCESS_UPDATED);
            var response = genericDataTypeRepository.save(request);
            
            if (response.getData() == null) {
              logger.error("Generic Data Type failed for: {}, {}", name, response.getMeta().getDetail());
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
      logger.error("Error Loading Generic Data Type {}", e.getMessage());
      throw e;
    }
    finally {
      if (this.getCsvParser() != null) this.getCsvParser().close();
    }
    logger.info("Completed Generic Data Type Load with {} persisted, {} deleted, and {} errors", persisted, deleted, errors);
  }
}