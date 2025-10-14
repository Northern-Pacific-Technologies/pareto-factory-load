package com.norpactech.pf.loader.service;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norpactech.pf.loader.dto.ProjectComponentPropertyPostApiRequest;
import com.norpactech.pf.loader.dto.ProjectComponentPropertyPutApiRequest;
import com.norpactech.pf.loader.dto.ProjectDeleteApiRequest;
import com.norpactech.pf.utils.ApiResponse;
import com.norpactech.pf.utils.Constant;
import com.norpactech.pf.utils.TextUtils;

public class LoadProjectComponentProperty extends BaseLoader {

  private static final Logger logger = LoggerFactory.getLogger(LoadProjectComponentProperty.class);

  public LoadProjectComponentProperty(String filePath, String fileName) throws Exception {
    super(filePath, fileName);
  }
  
  public void load() throws Exception {
    
    if (!isFileAvailable()) return;

    logger.info("Beginning Project Component Property Load from: " + getFullPath());
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
        var projectName = TextUtils.toString(csvRecord.get("project"));
        var projectComponentName = TextUtils.toString(csvRecord.get("project_component"));
        var sequence = TextUtils.toInteger(csvRecord.get("sequence"));
        var dataObjectFilter = TextUtils.toString(csvRecord.get("data_object_filter"));
        var propertyFilter = TextUtils.toString(csvRecord.get("property_filter"));
        
        var tenant = tenantRepository.findOne(tenantName);
        if (tenant == null) {
          logger.error("Tenant {} not found. Ignoring Project Component Property.", tenantName);
          continue;
        }
        
        var schema = schemaRepository.findOne(tenant.getId(), schemaName);
        if (schema == null) {
          logger.error("Schema {} not found. Ignoring Project Component Property.", schemaName);
          continue;
        }

        var project = projectRepository.findOne(tenant.getId(), schema.getId(), projectName);
        if (project == null) {
          logger.error("Project {} not found. Ignoring Project Component Property.", projectName);
          continue;
        }

        var projectComponent = projectComponentRepository.findOne(tenant.getId(), project.getId(), projectComponentName);        
        if (projectComponent == null) {
          logger.error("Project Component {} not found. Ignoring Project Component Property.", projectComponentName);
          continue;
        }        
        var projectComponentProperty = projectComponentPropertyRepository.findOne(tenant.getId(), projectComponent.getId(), dataObjectFilter, propertyFilter);        
        ApiResponse response = null; 

        if (action.startsWith("p")) {
          if (projectComponentProperty == null) {
            var request = new ProjectComponentPropertyPostApiRequest();
            request.setIdTenant(tenant.getId());
            request.setIdProjectComponent(projectComponent.getId());
            request.setSequence(sequence);
            request.setDataObjectFilter(dataObjectFilter);
            request.setPropertyFilter(propertyFilter);
            request.setCreatedBy(Constant.THIS_PROCESS_CREATED);
            response = projectComponentPropertyRepository.save(request);
          }
          else {
            var request = new ProjectComponentPropertyPutApiRequest();
            request.setId(projectComponentProperty.getId());
            request.setSequence(sequence);
            request.setDataObjectFilter(dataObjectFilter);
            request.setPropertyFilter(propertyFilter);
            request.setUpdatedAt(projectComponentProperty.getUpdatedAt());
            request.setUpdatedBy(Constant.THIS_PROCESS_UPDATED);
            response = projectComponentPropertyRepository.save(request);
          }
          if (response.getData() == null) {
            logger.error(this.getClass().getName() + " failed for: " + tenantName + " " + response.getMeta().getDetail());
            errors++;
          }
          else {
            persisted++;
          }
        }
        else if (action.startsWith("d") && projectComponentProperty != null) {
          ProjectDeleteApiRequest request = new ProjectDeleteApiRequest();
          request.setId(projectComponentProperty.getId());
          request.setUpdatedAt(projectComponentProperty.getUpdatedAt());
          request.setUpdatedBy(Constant.THIS_PROCESS_DELETED);
          projectRepository.delete(request);
          deleted++;
        }
      }
    }
    catch (Exception e) {
      logger.error("Error Loading Project Component Property {}", e.getMessage());
      throw e;
    }
    finally {
      if (this.getCsvParser() != null) this.getCsvParser().close();
    }
    logger.info("Completed Project Component Property Load with {} persisted, {} deleted, and {} errors", persisted, deleted, errors);
  }
}  