package com.norpactech.pf.loader.service;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norpactech.pf.loader.dto.RefTablesDeleteApiRequest;
import com.norpactech.pf.loader.dto.RefTablesPostApiRequest;
import com.norpactech.pf.loader.dto.RefTablesPutApiRequest;
import com.norpactech.pf.utils.ApiResponse;
import com.norpactech.pf.utils.Constant;
import com.norpactech.pf.utils.TextUtils;

public class LoadRefTables extends BaseLoader {

  private static final Logger logger = LoggerFactory.getLogger(LoadRefTables.class);

  public LoadRefTables(String filePath, String fileName) throws Exception {
    super(filePath, fileName);
  }
  
  public void load() throws Exception {
    
    if (!isFileAvailable()) return;

    logger.info("Beginning Reference Tables Load from: " + getFullPath());
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
        String refTableTypeName = TextUtils.toString(csvRecord.get("ref_table_type"));
        String name = TextUtils.toString(csvRecord.get("name"));
        String description = TextUtils.toString(csvRecord.get("description"));
        String value = TextUtils.toString(csvRecord.get("value"));
        Integer sequence = TextUtils.toInteger(csvRecord.get("sequence"));
        
        var tenant = tenantRepository.findOne(tenantName);
        if (tenant == null) {
          logger.error("Tenant {} not found. Ignoring Schema {}.", tenantName, name);
          continue;
        }
        var refTableType = refTableTypeRepository.findOne(tenant.getId(), refTableTypeName);
        if (refTableType == null) {
          logger.error("Reference Table Type {} not found. Ignoring Table Entry for {}.", refTableTypeName, name);
          continue;
        }
        var refTables = refTablesRepository.findOne(tenant.getId(), refTableType.getId(), name);
        ApiResponse response = null; 
        
        if (action.startsWith("p")) {
          if (refTables == null) {
            RefTablesPostApiRequest request = new RefTablesPostApiRequest();
            request.setIdTenant(tenant.getId());
            request.setIdRefTableType(refTableType.getId());
            request.setName(name);
            request.setDescription(description);
            request.setValue(value);
            request.setSequence(sequence);
            request.setCreatedBy(Constant.THIS_PROCESS_CREATED);
            response = refTablesRepository.save(request);
          }
          else {
            RefTablesPutApiRequest request = new RefTablesPutApiRequest();
            request.setId(refTables.getId());
            request.setName(name);
            request.setDescription(description);
            request.setValue(value);
            request.setSequence(sequence);
            request.setUpdatedAt(refTables.getUpdatedAt());
            request.setUpdatedBy(Constant.THIS_PROCESS_UPDATED);
            response = refTablesRepository.save(request);
          }
          if (response.getData() == null) {
            logger.error(this.getClass().getName() + " failed for: " + tenantName + " " + response.getMeta().getDetail());
            errors++;
          }
          else {
            persisted++;
          }
        }
        else if (action.startsWith("d") && refTables != null) {
          RefTablesDeleteApiRequest request = new RefTablesDeleteApiRequest();
          request.setId(refTables.getId());
          request.setUpdatedAt(refTables.getUpdatedAt());
          request.setUpdatedBy(Constant.THIS_PROCESS_DELETED);
          refTablesRepository.delete(request);
          deleted++;
        }
      }
    }
    catch (Exception e) {
      logger.error("Error Loading Schema: {}", e.getMessage());
      throw e;
    }
    finally {
      if (this.getCsvParser() != null) this.getCsvParser().close();
    }
    logger.info("Completed Schema Load with {} persisted, {} deleted, and {} errors", persisted, deleted, errors);
  }
}  