package com.norpactech.pf.loader.service;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norpactech.pf.loader.dto.GenericDataTypeAttributePostApiRequest;
import com.norpactech.pf.loader.dto.GenericDataTypeAttributePutApiRequest;
import com.norpactech.pf.loader.dto.UserDeleteApiRequest;
import com.norpactech.pf.loader.enums.EnumRefTableType;
import com.norpactech.pf.loader.model.RefTableType;
import com.norpactech.pf.loader.model.RefTables;
import com.norpactech.pf.utils.ApiResponse;
import com.norpactech.pf.utils.Constant;
import com.norpactech.pf.utils.TextUtils;

public class LoadGenericDataTypeAttribute extends BaseLoader {

  private static final Logger logger = LoggerFactory.getLogger(LoadGenericDataTypeAttribute.class);

  public LoadGenericDataTypeAttribute(String filePath, String fileName) throws Exception {
    super(filePath, fileName);
  }
  
  public void load() throws Exception {
    
    if (!isFileAvailable()) return;

    logger.info("Beginning Generic Data Type Attribute Load from: " + getFullPath());
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
        var genericDataTypeName = TextUtils.toString(csvRecord.get("generic_data_type"));
        var attributeDataType = TextUtils.toString(csvRecord.get("rt_attr_data_type"));
        var name = TextUtils.toString(csvRecord.get("name"));
        var description = TextUtils.toString(csvRecord.get("description"));

        var tenant = tenantRepository.findOne(tenantName);
        if (tenant == null) {
          logger.error("Tenant {} not found. Ignoring Generic Data Type Attribute {}.", tenantName, name);
          continue;
        }

        var schema = schemaRepository.findOne(tenant.getId(), schemaName);
        if (schema == null) {
          logger.error("Schema {} not found. Ignoring Generic Data Type Attribute {}.", schemaName, name);
          continue;
        }        
        
        var genericDataType = genericDataTypeRepository.findOne(tenant.getId(), genericDataTypeName);
        if (genericDataType == null) {
          logger.error("Generic Data Type {} not found. Ignoring Generic Data Type Attribute {}.", genericDataTypeName, name);
          continue;
        }        

        RefTableType refTableType = refTableTypeRepository.findOne (tenant.getId(), EnumRefTableType.ATTR_DATA_TYPE.name());
        if (refTableType == null) {
          logger.error("Reference Table Type {} not found. Ignoring Generic Data Type Attribute {}.", EnumRefTableType.ATTR_DATA_TYPE.name(), name);
          continue;
        }        
        
        RefTables refTables = refTablesRepository.findOne(tenant.getId(), refTableType.getId(), attributeDataType);
        if (refTables == null) {
          logger.error("Reference Table Entry {} not found. Ignoring Generic Data Type Attribute {}.", attributeDataType, name);
          continue;
        }  
        
        var genericDataTypeAttribute = genericDataTypeAttributeRepository.findOne(tenant.getId(), genericDataType.getId(), name);
        ApiResponse response = null; 
        
        if (action.startsWith("p")) {
          if (genericDataTypeAttribute == null) {
            var request = new GenericDataTypeAttributePostApiRequest();
            request.setIdTenant(tenant.getId());
            request.setIdGenericDataType(genericDataType.getId());
            request.setIdRtAttrDataType(refTables.getId());
            request.setName(name);
            request.setDescription(description);
            request.setCreatedBy(Constant.THIS_PROCESS_CREATED);
            response = genericDataTypeAttributeRepository.save(request);                    
          }
          else {
            var request = new GenericDataTypeAttributePutApiRequest();
            request.setId(genericDataTypeAttribute.getId());
            request.setIdRtAttrDataType(refTables.getId());
            request.setName(name);
            request.setDescription(description);
            request.setUpdatedAt(genericDataTypeAttribute.getUpdatedAt());
            request.setUpdatedBy(Constant.THIS_PROCESS_UPDATED);
            response = genericDataTypeAttributeRepository.save(request);
          }
          if (response.getData() == null) {
            if (response.getError() != null) {
              logger.error(response.getError().toString());
            }
            else {
              logger.error(this.getClass().getName() + " failed for: " + name + " " + response.getMeta().getDetail());
            }
            errors++;
          }
          else {
            persisted++;
          }          
        }
        else if (action.startsWith("d") && genericDataTypeAttribute != null) {
          var request = new UserDeleteApiRequest();
          request.setId(genericDataType.getId());
          request.setUpdatedAt(genericDataType.getUpdatedAt());
          request.setUpdatedBy(Constant.THIS_PROCESS_DELETED);
          userRepository.delete(request);
          deleted++;
        }
      }
    }
    catch (Exception e) {
      logger.error("Error Loading Generic Data Type Attribute {}", e.getMessage());
      throw e;
    }
    finally {
      if (this.getCsvParser() != null) this.getCsvParser().close();
    }
    logger.info("Completed Generic Data Type Attribute Load with {} persisted, {} deleted, and {} errors", persisted, deleted, errors);
  }
}