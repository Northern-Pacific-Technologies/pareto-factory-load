package com.norpactech.pf.loader.service;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norpactech.nc.utils.TextUtils;
import com.norpactech.pf.loader.dto.RefTablesPostApiRequest;
import com.norpactech.pf.loader.dto.RefTablesPutApiRequest;
import com.norpactech.pf.utils.Constant;

public class LoadRefTables extends BaseLoader {

  private static final Logger logger = LoggerFactory.getLogger(LoadRefTables.class);

  public LoadRefTables(String filePath, String fileName) throws Exception {
    super(filePath, fileName);
  }
  
  public void load() throws Exception {
    
    if (!isFileAvailable()) return;

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
        var refTableTypeName = TextUtils.toString(csvRecord.get("ref_table_type"));
        var name = TextUtils.toString(csvRecord.get("name"));
        var description = TextUtils.toString(csvRecord.get("description"));
        var value = TextUtils.toString(csvRecord.get("value"));
        var sequence = TextUtils.toInteger(csvRecord.get("sequence"));
        
        var tenant = tenantRepository.findOne(tenantName);
        if (tenant == null) {
          logger.error("Tenant {} not found. Ignoring RefTables {}.", tenantName, name);
          continue;
        }
        
        var refTableType = refTableTypeRepository.findOne(tenant.getId(), refTableTypeName);
        if (refTableType == null) {
          logger.error("Reference Table Type {} not found. Ignoring RefTables Entry for {}.", refTableTypeName, name);
          continue;
        }
        var refTables = refTablesRepository.findOne(tenant.getId(), refTableType.getId(), name);
        
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
            var response = refTablesRepository.save(request);
            
            if (response.getData() == null) {
              logger.error("RefTables failed for: {}, {}", name, response.getMeta().getDetail());
              errors++;
            }
            else {
              persisted++;
            }
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
            var response = refTablesRepository.save(request);
            
            if (response.getData() == null) {
              logger.error("RefTables failed for: {}, {}", name, response.getMeta().getDetail());
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
      logger.error("Error Loading Schema: {}", e.getMessage());
      throw e;
    }
    finally {
      if (this.getCsvParser() != null) this.getCsvParser().close();
    }
    logger.info("Completed Schema Load with {} persisted, {} deleted, and {} errors", persisted, deleted, errors);
  }
}  