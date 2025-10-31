package com.norpactech.pf.loader.service;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norpactech.nc.utils.TextUtils;
import com.norpactech.pf.loader.dto.ValidationPostApiRequest;
import com.norpactech.pf.loader.dto.ValidationPutApiRequest;
import com.norpactech.pf.loader.enums.EnumRefTableType;
import com.norpactech.pf.loader.model.RefTableType;
import com.norpactech.pf.loader.model.RefTables;
import com.norpactech.pf.utils.Constant;

public class LoadValidation extends BaseLoader {

  private static final Logger logger = LoggerFactory.getLogger(LoadValidation.class);

  public LoadValidation(String filePath, String fileName) throws Exception {
    super(filePath, fileName);
  }
  
  public void load() throws Exception {
    
    if (!isFileAvailable()) return;

    logger.info("Beginning Validation Load from: " + getFullPath());
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
        var name = TextUtils.toString(csvRecord.get("name"));
        var description = TextUtils.toString(csvRecord.get("description"));
        var validationType = TextUtils.toString(csvRecord.get("validation_type"));
        var errorMsg = TextUtils.toString(csvRecord.get("error_msg"));
        var expression = TextUtils.toString(csvRecord.get("expression"));

        var tenant = tenantRepository.findOne(tenantName);
        if (tenant == null) {
          logger.error("Tenant {} not found. Ignoring Validation {}.", tenantName, name);
          continue;
        }
        
        RefTableType refTableType = refTableTypeRepository.findOne(tenant.getId(), EnumRefTableType.VALIDATION_TYPE.name());
        if (refTableType == null) {
          logger.error("Reference Table Type {} not found. Ignoring Validation {}.", EnumRefTableType.VALIDATION_TYPE.name(), name);
          continue;
        }        
        
        RefTables refTables = refTablesRepository.findOne(tenant.getId(), refTableType.getId(), validationType);
        if (refTables == null) {
          logger.error("Reference Table Entry {} not found. Ignoring Validation {}.", validationType, name);
          continue;
        }  
        var validation = validationRepository.findOne(tenant.getId(), name);
        
        if (action.startsWith("p")) {
          if (validation == null) {
            var request = new ValidationPostApiRequest();
            request.setIdTenant(tenant.getId());
            request.setIdRtValidationType(refTables.getId());
            request.setName(name);
            request.setDescription(description);
            request.setErrorMsg(errorMsg);
            request.setExpression(expression);
            request.setCreatedBy(Constant.THIS_PROCESS_CREATED);
            var response = validationRepository.save(request);
            
            if (response.getData() == null) {
              logger.error("Validation failed for: {}, {}", name, response.getMeta().getDetail());
              errors++;
            }
            else {
              persisted++;
            }
          }
          else {
            var request = new ValidationPutApiRequest();
            request.setId(validation.getId());
            request.setIdRtValidationType(refTables.getId());
            request.setName(name);
            request.setDescription(description);
            request.setErrorMsg(errorMsg);
            request.setExpression(expression);
            request.setUpdatedAt(validation.getUpdatedAt());
            request.setUpdatedBy(Constant.THIS_PROCESS_UPDATED);
            var response = validationRepository.save(request);
            
            if (response.getData() == null) {
              logger.error("Validation failed for: {}, {}", name, response.getMeta().getDetail());
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
      logger.error("Error Loading Validation {}", e.getMessage());
      throw e;
    }
    finally {
      if (this.getCsvParser() != null) this.getCsvParser().close();
    }
    logger.info("Completed Validation Load with {} persisted, {} deleted, and {} errors", persisted, deleted, errors);
  }
}