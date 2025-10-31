package com.norpactech.pf.loader.service;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norpactech.nc.api.utils.ApiResponse;
import com.norpactech.nc.utils.TextUtils;
import com.norpactech.pf.loader.dto.DataIndexPropertyPostApiRequest;
import com.norpactech.pf.loader.dto.DataIndexPropertyPutApiRequest;
import com.norpactech.pf.loader.enums.EnumRefTableType;
import com.norpactech.pf.utils.Constant;

public class LoadDataIndexProperty extends BaseLoader {

  private static final Logger logger = LoggerFactory.getLogger(LoadDataIndexProperty.class);

  public LoadDataIndexProperty(String filePath, String fileName) throws Exception {
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
        var objectName = TextUtils.toString(csvRecord.get("object"));
        var dataIndexName = TextUtils.toString(csvRecord.get("data_index"));
        var propertyName = TextUtils.toString(csvRecord.get("property"));
        var sortOrder = TextUtils.toString(csvRecord.get("sort_order"));
        var sequence = TextUtils.toInteger(csvRecord.get("sequence"));

        var tenant = tenantRepository.findOne(tenantName);
        if (tenant == null) {
          logger.error("Tenant {} not found. Ignoring Data Index Property {}.", tenantName, dataIndexName);
          continue;
        }
        
        var schema = schemaRepository.findOne(tenant.getId(), schemaName);
        if (schema == null) {
          logger.error("Schema {} not found. Ignoring Data Index Property {}.", schemaName, dataIndexName);
          continue;
        }

        var dataObject = dataObjectRepository.findOne(tenant.getId(), schema.getId(), objectName);
        if (dataObject == null) {
          logger.error("Data Object {} not found. Ignoring Data Index Property {}.", objectName, dataIndexName);
          continue;
        }

        var property = propertyRepository.findOne(tenant.getId(), dataObject.getId(), propertyName);
        if (property == null) {
          logger.error("Data Object Property {} not found. Ignoring Data Index Property {}.", objectName, dataIndexName);
          continue;
        }
        
        var dataIndex = dataIndexRepository.findOne(tenant.getId(), dataObject.getId(), dataIndexName);
        if (dataIndex == null) {
          logger.error("Data Index {} not found. Ignoring Data Index Property {}.", dataIndexName, propertyName);
          continue;
        }
        
        var refTableSortOrder = refTableTypeRepository.findOne(tenant.getId(), EnumRefTableType.SORT_ORDER.getName());
        if (refTableSortOrder == null) {
          logger.error("Sort Order Table Type {} not found. Ignoring Data Index Property {}.", EnumRefTableType.SORT_ORDER.getName(), propertyName);
          continue;
        }

        var refTablesSortOrder = refTablesRepository.findOne(tenant.getId(), refTableSortOrder.getId(), sortOrder);
        if (refTablesSortOrder == null) {
          logger.error("Sort Order Value {} not found. Ignoring Data Index Property {}.", sortOrder, propertyName);
          continue;
        }        

        var dataIndexProperty = dataIndexPropertyRepository.findOne(tenant.getId(), dataIndex.getId(), property.getId());
        ApiResponse response = null; 
        
        if (action.startsWith("p")) {
          if (dataIndexProperty == null) {
            var request = new DataIndexPropertyPostApiRequest();
            request.setIdTenant(tenant.getId());
            request.setIdDataIndex(dataIndex.getId());
            request.setIdProperty(property.getId());
            request.setIdRtSortOrder(refTablesSortOrder.getId());            
            request.setSequence(sequence);
            request.setCreatedBy(Constant.THIS_PROCESS_CREATED);
            response = dataIndexPropertyRepository.save(request);
            
            if (response.getData() == null) {
              logger.error("Data Index Property failed for: {}, {}", propertyName, response.getMeta().getDetail());
              errors++;
            }
            else {
              persisted++;
            }
          }
          else {
            var request = new DataIndexPropertyPutApiRequest();
            request.setId(dataIndexProperty.getId());
            request.setIdProperty(property.getId());            
            request.setIdRtSortOrder(refTablesSortOrder.getId());            
            request.setSequence(sequence);
            request.setUpdatedAt(dataIndexProperty.getUpdatedAt());
            request.setUpdatedBy(Constant.THIS_PROCESS_UPDATED);
            response = dataIndexPropertyRepository.save(request);
            
            if (response.getData() == null) {
              logger.error("Data Index Property failed for: {}, {}", propertyName, response.getMeta().getDetail());
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
      logger.error("Error Loading Data Index Property: {}", e.getMessage());
      throw e;
    }
    finally {
      if (this.getCsvParser() != null) this.getCsvParser().close();
    }
    logger.info("Completed Data Index Property Load with {} persisted, {} deleted, and {} errors", persisted, deleted, errors);
  }
}  