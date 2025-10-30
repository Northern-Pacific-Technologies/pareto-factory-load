package com.norpactech.pf.loader.service;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norpactech.nc.api.utils.ApiResponse;
import com.norpactech.nc.utils.TextUtils;
import com.norpactech.pf.loader.dto.RefTableTypeDeleteApiRequest;
import com.norpactech.pf.loader.dto.RefTableTypePostApiRequest;
import com.norpactech.pf.loader.dto.RefTableTypePutApiRequest;
import com.norpactech.pf.utils.Constant;

public class LoadRefTableType extends BaseLoader {

  private static final Logger logger = LoggerFactory.getLogger(LoadRefTableType.class);

  public LoadRefTableType(String filePath, String fileName) throws Exception {
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
        String action = TextUtils.toString(csvRecord.get("action")).toLowerCase();
        String tenantName = TextUtils.toString(csvRecord.get("tenant"));
        String name = TextUtils.toString(csvRecord.get("name"));
        String description = TextUtils.toString(csvRecord.get("description"));
        
        var tenant = tenantRepository.findOne(tenantName);
        if (tenant == null) {
          logger.error("Tenant {} not found. Ignoring Schema {}.", tenantName, name);
          continue;
        }
        var refTableType = refTableTypeRepository.findOne(tenant.getId(), name);
        ApiResponse response = null; 
            
        if (action.startsWith("p")) {
          if (refTableType == null) {
            RefTableTypePostApiRequest request = new RefTableTypePostApiRequest();
            request.setIdTenant(tenant.getId());
            request.setName(name);
            request.setDescription(description);
            request.setCreatedBy(Constant.THIS_PROCESS_CREATED);
            response = refTableTypeRepository.save(request);
            if (response.getData() == null) {
              logger.error(this.getClass().getName() + " failed for: " + tenantName + " " + response.getMeta().getDetail());
              errors++;
            }
            else {
              persisted++;
            }
          }
          else {
            RefTableTypePutApiRequest request = new RefTableTypePutApiRequest();
            request.setId(refTableType.getId());
            request.setName(name);
            request.setDescription(description);
            request.setUpdatedAt(refTableType.getUpdatedAt());
            request.setUpdatedBy(Constant.THIS_PROCESS_UPDATED);
            response = refTableTypeRepository.save(request);
          }
          if (response.getData() == null) {
            logger.error(this.getClass().getName() + " failed for: " + tenantName + " " + response.getMeta().getDetail());
            errors++;
          }
          else {
            persisted++;
          }
        }
        else if (action.startsWith("d") && refTableType != null) {
          RefTableTypeDeleteApiRequest request = new RefTableTypeDeleteApiRequest();
          request.setId(refTableType.getId());
          request.setUpdatedAt(refTableType.getUpdatedAt());
          request.setUpdatedBy(Constant.THIS_PROCESS_DELETED);
          refTableTypeRepository.delete(request);
          deleted++;
        }
      }
    }
    catch (Exception e) {
      throw new Exception("Error Loading Reference Table Type: ", e);
    }
    finally {
      if (this.getCsvParser() != null) this.getCsvParser().close();
    }
    logger.info("Completed Reference Table Type Load with {} persisted, {} deleted, and {} errors", persisted, deleted, errors);
  }
}  