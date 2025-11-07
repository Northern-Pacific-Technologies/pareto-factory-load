package com.norpactech.pf.loader.service;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norpactech.nc.config.tenant.TenantContext;
import com.norpactech.nc.utils.TextUtils;
import com.norpactech.pf.loader.dto.TenantPostApiRequest;
import com.norpactech.pf.loader.dto.TenantPutApiRequest;
import com.norpactech.pf.loader.model.Tenant;
import com.norpactech.pf.utils.Constant;

public class LoadTenant extends BaseLoader {

  private static final Logger logger = LoggerFactory.getLogger(LoadTenant.class);

  public LoadTenant(String filePath, String fileName) throws Exception {
    super(filePath, fileName);
  }
  
  public void load() throws Exception {
    
    if (!isFileAvailable()) return;
    
    int persisted = 0;
    int deleted = 0;
    int errors = 0;

    String tenantName = null;
    Boolean processedTenant = false;
    
    try {
      for (CSVRecord csvRecord : this.getCsvParser()) {
        if (isComment(csvRecord)) {
          continue;
        }
        if (processedTenant) {
          logger.warn("Loader supports only one tenant per run. Additional tenants found after: " + tenantName);
          break;
        }
        var action = csvRecord.get("action").toLowerCase();
            tenantName = TextUtils.toString(csvRecord.get("name"));
        var description = TextUtils.toString(csvRecord.get("description"));
        var copyright = TextUtils.toString(csvRecord.get("copyright"));
        var timeZone = TextUtils.toString(csvRecord.get("time_zone"));
                
        Tenant tenant = tenantRepositoryEx.findOne(tenantName);
        if (tenant != null) {
          // Set the Tenant Context for subsequent operations
          TenantContext.setId(tenant.getId());
          logger.info("Tenant Context Set: {}", tenant.getId().toString());
        }

        if (action.startsWith("p")) {
          if (tenant == null) {
            TenantPostApiRequest request = new TenantPostApiRequest();
            request.setName(tenantName);
            request.setDescription(description);
            request.setCopyright(copyright);
            request.setTimeZone(timeZone);
            request.setCreatedBy(Constant.THIS_PROCESS_CREATED);
            var response = tenantRepositoryEx.save(request);
            
            if (response.getData() == null) {
              logger.error(this.getClass().getName() + " failed for: " + tenantName + " " + response.getMeta().getDetail());
              errors++;
            }
            else {
              // Set the Tenant Context for subsequent operations
              tenant = tenantRepositoryEx.findOne(tenantName);
              if (tenant != null) {
                TenantContext.setId(tenant.getId());
                logger.info("Tenant Context Set: {}", tenant.getId().toString());
                processedTenant = true;
                persisted++;
              }
              else {
                throw new Exception("Tenant was created but cannot be retrieved!: " + tenantName);
              }
            }
          }
          else {
            TenantPutApiRequest request = new TenantPutApiRequest();
            request.setId(tenant.getId());
            request.setName(tenantName);
            request.setDescription(description);
            request.setCopyright(copyright);
            request.setTimeZone(timeZone);
            request.setUpdatedAt(tenant.getUpdatedAt());
            request.setUpdatedBy(Constant.THIS_PROCESS_UPDATED);
            var response = tenantRepository.save(request);
            
            if (response.getData() == null) {
              logger.error("Tenant failed for: {}, {}", tenantName, response.getMeta().getDetail());
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
      logger.error("Error Loading Tenant {}", e.getMessage());
      throw e;
    }
    finally {
      if (this.getCsvParser() != null) this.getCsvParser().close();
    }
    logger.info("Completed Tenant Load with {} persisted, {} deleted, and {} errors", persisted, deleted, errors);
  }
}  