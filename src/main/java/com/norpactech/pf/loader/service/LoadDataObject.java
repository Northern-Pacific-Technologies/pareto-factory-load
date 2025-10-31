package com.norpactech.pf.loader.service;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norpactech.nc.utils.TextUtils;
import com.norpactech.pf.loader.dto.DataObjectPostApiRequest;
import com.norpactech.pf.loader.dto.DataObjectPutApiRequest;
import com.norpactech.pf.utils.Constant;

public class LoadDataObject extends BaseLoader {

  private static final Logger logger = LoggerFactory.getLogger(LoadDataObject.class);

  public LoadDataObject(String filePath, String fileName) throws Exception {
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
        var schemaName = TextUtils.toString(csvRecord.get("schema"));
        var name = TextUtils.toString(csvRecord.get("name"));
        var description = TextUtils.toString(csvRecord.get("description"));            
        var hasIdentifier = TextUtils.toBoolean(csvRecord.get("has_identifier"));
        var hasTenancy = TextUtils.toBoolean(csvRecord.get("has_tenancy"));
        var hasAudit = TextUtils.toBoolean(csvRecord.get("has_audit"));
        var hasActive = TextUtils.toBoolean(csvRecord.get("has_active"));
        
        var tenant = tenantRepository.findOne(tenantName);
        if (tenant == null) {
          logger.error("Tenant {} not found. Ignoring Data Object {}.", tenantName, name);
          continue;
        }
        var schema = schemaRepository.findOne(tenant.getId(), schemaName);
        if (schema == null) {
          logger.error("Schema {} not found. Ignoring Data Object {}.", schemaName, name);
          continue;
        }
        var dataObject = dataObjectRepository.findOne(tenant.getId(), schema.getId(), name);

        if (action.startsWith("p")) {
          if (dataObject == null) {
            var request = new DataObjectPostApiRequest();
            request.setIdTenant(tenant.getId());
            request.setIdSchema(schema.getId());
            request.setName(name);
            request.setDescription(description);            
            request.setHasIdentifier(hasIdentifier);
            request.setHasTenancy(hasTenancy);
            request.setHasAudit(hasAudit);
            request.setHasActive(hasActive);
            request.setCreatedBy(Constant.THIS_PROCESS_CREATED);
            var response = dataObjectRepository.save(request);   

            if (response.getData() == null) {
              logger.error("Data Index Property failed for: {}, {}", name, response.getMeta().getDetail());
              errors++;
            }
            else {
              persisted++;
            }
          }
          else {
            var request = new DataObjectPutApiRequest();
            request.setId(dataObject.getId());
            request.setName(name);
            request.setDescription(description);            
            request.setHasIdentifier(hasIdentifier);
            request.setHasTenancy(hasTenancy);
            request.setHasAudit(hasAudit);
            request.setHasActive(hasActive);
            request.setUpdatedAt(dataObject.getUpdatedAt());
            request.setUpdatedBy(Constant.THIS_PROCESS_UPDATED);
            var response = dataObjectRepository.save(request);
            
            if (response.getData() == null) {
              logger.error("Data Index Property failed for: {}, {}", name, response.getMeta().getDetail());
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
      logger.error("Error Loading Data Object {}", e.getMessage());
      throw e;
    }
    finally {
      if (this.getCsvParser() != null) this.getCsvParser().close();
    }
    logger.info("Completed Data Object Load with {} persisted, {} deleted, and {} errors", persisted, deleted, errors);
  }
}  