package com.norpactech.pf.loader.service;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norpactech.nc.api.utils.ApiResponse;
import com.norpactech.nc.utils.TextUtils;
import com.norpactech.pf.loader.dto.DataIndexPostApiRequest;
import com.norpactech.pf.loader.dto.DataIndexPutApiRequest;
import com.norpactech.pf.loader.enums.EnumRefTableType;
import com.norpactech.pf.loader.model.RefTableType;
import com.norpactech.pf.loader.model.RefTables;
import com.norpactech.pf.utils.Constant;

public class LoadDataIndex extends BaseLoader {

  private static final Logger logger = LoggerFactory.getLogger(LoadDataIndex.class);

  public LoadDataIndex(String filePath, String fileName) throws Exception {
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
        var indexType = TextUtils.toString(csvRecord.get("index_type"));
        var name = TextUtils.toString(csvRecord.get("name"));

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
        
        var dataObject = dataObjectRepository.findOne(tenant.getId(), schema.getId(), objectName);
        if (dataObject == null) {
          logger.error("Data Object {} not found. Ignoring Index {}.", objectName, name);
          continue;
        }
        
        RefTableType refTableIndex = refTableTypeRepository.findOne(tenant.getId(), EnumRefTableType.INDEX_TYPE.getName());
        if (refTableIndex == null) {
          logger.error("Data Index Table Type {} not found. Ignoring DataIndex {}.", EnumRefTableType.INDEX_TYPE.getName(), name);
          continue;
        }
        RefTables refTablesIndexType = null;
        if (indexType != null) {
          refTablesIndexType = refTablesRepository.findOne(tenant.getId(), refTableIndex.getId(), indexType);
          if (refTablesIndexType == null) {
            logger.error("Data Index Type {} not found. Ignoring DataIndex {}.", indexType, name);
            continue;
          }        
        }
        var dataIndex = dataIndexRepository.findOne(tenant.getId(), dataObject.getId(), name);
        ApiResponse response = null; 
        
        if (action.startsWith("p")) {
          if (dataIndex == null) {
            var request = new DataIndexPostApiRequest();
            request.setIdTenant(tenant.getId());
            request.setIdDataObject(dataObject.getId());
            request.setIdRtIndexType(refTablesIndexType.getId());
            request.setName(name);
            request.setCreatedBy(Constant.THIS_PROCESS_CREATED);
            response = dataIndexRepository.save(request);

            if (response.getData() == null) {
              logger.error("Data Index failed for: {}, {}", name, response.getMeta().getDetail());
              errors++;
            }
            else {
              persisted++;
            }
          }
          else {
            var request = new DataIndexPutApiRequest();
            request.setId(dataIndex.getId());
            request.setIdRtIndexType(refTablesIndexType.getId());
            request.setName(name);
            request.setUpdatedAt(dataIndex.getUpdatedAt());
            request.setUpdatedBy(Constant.THIS_PROCESS_UPDATED);
            response = dataIndexRepository.save(request);

            if (response.getData() == null) {
              logger.error("Data Index failed for: {}, {}", name, response.getMeta().getDetail());
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
      logger.error("Error Loading Data Index: {}", e.getMessage());
      throw e;
    }
    finally {
      if (this.getCsvParser() != null) this.getCsvParser().close();
    }
    logger.info("Completed Data Index Load with {} persisted, {} deleted, and {} errors", persisted, deleted, errors);
  }
}  