package com.norpactech.pf.loader.service;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norpactech.nc.utils.TextUtils;
import com.norpactech.pf.loader.dto.ProjectPostApiRequest;
import com.norpactech.pf.loader.dto.ProjectPutApiRequest;
import com.norpactech.pf.utils.Constant;

public class LoadProject extends BaseLoader {

  private static final Logger logger = LoggerFactory.getLogger(LoadProject.class);

  public LoadProject(String filePath, String fileName) throws Exception {
    super(filePath, fileName);
  }
  
  public void load() throws Exception {
    
    if (!isFileAvailable()) return;

    logger.info("Beginning Project Load from: " + getFullPath());
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
        var domain = TextUtils.toString(csvRecord.get("domain"));
        var artifact = TextUtils.toString(csvRecord.get("artifact"));
        
        var tenant = tenantRepository.findOne(tenantName);
        if (tenant == null) {
          logger.error("Tenant {} not found. Ignoring Project {}.", tenantName, name);
          continue;
        }
        
        var schema = schemaRepository.findOne(tenant.getId(), schemaName);
        if (schema == null) {
          logger.error("Schema {} not found. Ignoring Project {}.", schemaName, name);
          continue;
        }
        var project = projectRepository.findOne(tenant.getId(), schema.getId(), name);
            
        if (action.startsWith("p")) {
          if (project == null) {
            var request = new ProjectPostApiRequest();
            request.setIdTenant(tenant.getId());
            request.setIdSchema(schema.getId());
            request.setName(name);
            request.setDescription(description);
            request.setDomain(domain);
            request.setArtifact(artifact);
            request.setCreatedBy(Constant.THIS_PROCESS_CREATED);
            var response = projectRepository.save(request);
            
            if (response.getData() == null) {
              logger.error("Project failed for: {}, {}", name, response.getMeta().getDetail());
              errors++;
            }
            else {
              persisted++;
            }
          }
          else {
            var request = new ProjectPutApiRequest();
            request.setId(project.getId());
            request.setName(name);
            request.setDescription(description);
            request.setDomain(domain);
            request.setArtifact(artifact);
            request.setUpdatedAt(project.getUpdatedAt());
            request.setUpdatedBy(Constant.THIS_PROCESS_UPDATED);
            var response = projectRepository.save(request);
            
            if (response.getData() == null) {
              logger.error("Project failed for: {}, {}", name, response.getMeta().getDetail());
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
      logger.error("Error Loading Project {}", e.getMessage());
      throw e;
    }
    finally {
      if (this.getCsvParser() != null) this.getCsvParser().close();
    }
    logger.info("Completed Project Load with {} persisted, {} deleted, and {} errors", persisted, deleted, errors);
  }
}  