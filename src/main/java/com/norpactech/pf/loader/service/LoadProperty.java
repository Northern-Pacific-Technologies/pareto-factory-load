package com.norpactech.pf.loader.service;
import java.util.UUID;

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norpactech.nc.utils.TextUtils;
import com.norpactech.pf.loader.dto.CardinalityPostApiRequest;
import com.norpactech.pf.loader.dto.CardinalityPutApiRequest;
import com.norpactech.pf.loader.dto.PropertyPostApiRequest;
import com.norpactech.pf.loader.dto.PropertyPutApiRequest;
import com.norpactech.pf.loader.enums.EnumRefTableType;
import com.norpactech.pf.loader.model.Cardinality;
import com.norpactech.pf.loader.model.DataObject;
import com.norpactech.pf.loader.model.GenericPropertyType;
import com.norpactech.pf.loader.model.RefTableType;
import com.norpactech.pf.loader.model.RefTables;
import com.norpactech.pf.loader.model.Validation;
import com.norpactech.pf.utils.Constant;

public class LoadProperty extends BaseLoader {

  private static final Logger logger = LoggerFactory.getLogger(LoadProperty.class);

  public LoadProperty(String filePath, String fileName) throws Exception {
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
        var action = csvRecord.get("action").toLowerCase();
        var tenantName = TextUtils.toString(csvRecord.get("tenant"));
        var schemaName = TextUtils.toString(csvRecord.get("schema"));
        var dataObjectName = TextUtils.toString(csvRecord.get("object"));
        var dataTypeName = TextUtils.toString(csvRecord.get("data_type"));
        var sequence = TextUtils.toInteger(csvRecord.get("sequence"));
        var name = TextUtils.toString(csvRecord.get("name"));
        var description = TextUtils.toString(csvRecord.get("description"));
        var references = TextUtils.toString(csvRecord.get("references"));
        var cardinality = TextUtils.toString(csvRecord.get("cardinality"));
        
        Boolean isUpdatable = true;
        if (StringUtils.isNotEmpty(csvRecord.get("is_updatable"))) {
          isUpdatable = TextUtils.toBoolean(csvRecord.get("is_updatable"));
        }
        Boolean fkViewable = false;
        if (StringUtils.isNotEmpty(csvRecord.get("fk_viewable"))) {
          fkViewable = TextUtils.toBoolean(csvRecord.get("fk_viewable"));
        }
        var hasReferentialAction = TextUtils.toBoolean(csvRecord.get("has_referential_action"));
        var length = TextUtils.toInteger(csvRecord.get("length"));
        var scale = TextUtils.toInteger(csvRecord.get("scale"));
        
        Boolean isNullable = true;
        if (StringUtils.isNotEmpty(csvRecord.get("is_nullable"))) {
          isNullable = TextUtils.toBoolean(csvRecord.get("is_nullable"));
        }
        var defaultValue = TextUtils.toString(csvRecord.get("default_value"));
        var validationName = TextUtils.toString(csvRecord.get("validation"));
        
        var tenant = tenantRepository.findOne(tenantName);
        if (tenant == null) {
          logger.error("Tenant {} not found. Ignoring Property {}.", tenantName, name);
          continue;
        }

        var schema = schemaRepository.findOne(tenant.getId(), schemaName);
        if (schema == null) {
          logger.error("Schema {} not found. Ignoring Property {}.", schemaName, name);
          continue;
        }
        
        var dataObject = dataObjectRepository.findOne(tenant.getId(), schema.getId(), dataObjectName);
        if (dataObject == null) {
          logger.error("Data Object {} not found. Ignoring Property {}.", dataObjectName, name);
          continue;
        }
        Validation validation = null;
        if (StringUtils.isNotEmpty(validationName)) {
          validationRepository.findOne(tenant.getId(), validationName);
        }
        // The generic data type is either in the property or property type (domain)
        UUID idGenericDataType = null;
        var genericDataType = genericDataTypeRepository.findOne(tenant.getId(), dataTypeName);

        GenericPropertyType genericPropertyType = null;
        if (genericDataType == null) {
          genericPropertyType = genericPropertyTypeRepository.findOne(tenant.getId(), dataTypeName);
          if (genericPropertyType != null) {
            idGenericDataType = genericPropertyType.getIdGenericDataType();
          }
        }
        else {
          idGenericDataType = genericDataType.getId();
        }

        var property = propertyRepository.findOne(tenant.getId(), dataObject.getId(), name);
        
        if (action.startsWith("p")) {
          if (property == null) {
            var request = new PropertyPostApiRequest();
            request.setIdTenant(tenant.getId());
            request.setIdDataObject(dataObject.getId());
            request.setIdGenericDataType(idGenericDataType);
            request.setIdGenericPropertyType(genericPropertyType != null ? genericPropertyType.getId() : null);
            request.setIdValidation(validation != null ? validation.getId() : null);
            request.setSequence(sequence);
            request.setName(name);
            request.setDescription(description);
            request.setIsUpdatable(isUpdatable);
            request.setFkViewable(fkViewable);
            request.setLength(length);
            request.setScale(scale);
            request.setIsNullable(isNullable);            
            request.setDefaultValue(defaultValue);
            request.setCreatedBy(Constant.THIS_PROCESS_CREATED);
            var response = propertyRepository.save(request);    
            
            if (response.getData() == null) {
              logger.error("Property failed for: {}, {}", name, response.getMeta().getDetail());
              errors++;
              continue;
            }
            else {
              persisted++;
            }
          }
          else {
            var request = new PropertyPutApiRequest();
            request.setId(property.getId());
            request.setIdGenericDataType(idGenericDataType);
            request.setIdGenericPropertyType(genericPropertyType != null ? genericPropertyType.getId() : null);
            request.setIdValidation(validation != null ? validation.getId() : null);
            request.setSequence(sequence);
            request.setName(name);
            request.setDescription(description);
            request.setIsUpdatable(isUpdatable);
            request.setFkViewable(fkViewable);
            request.setLength(length);
            request.setScale(scale);
            request.setIsNullable(isNullable);            
            request.setDefaultValue(defaultValue);
            request.setUpdatedAt(property.getUpdatedAt());
            request.setUpdatedBy(Constant.THIS_PROCESS_UPDATED);
            var response = propertyRepository.save(request);
            
            if (response.getData() == null) {
              logger.error("Property failed for: {}, {}", name, response.getMeta().getDetail());
              errors++;
              continue;
            }
            else {
              persisted++;
            }
          }
          // Load the cardinality
          if (StringUtils.isNotEmpty(references)) {   
            property = propertyRepository.findOne(tenant.getId(), dataObject.getId(), name); // Refresh
            if (property == null) {
              continue;
            }
            DataObject cardinalityDataObject = dataObjectRepository.findOne(tenant.getId(), schema.getId(), references);

            if (cardinalityDataObject == null) {
              continue;
            }
            RefTableType refTableCardinality = refTableTypeRepository.findOne(tenant.getId(), EnumRefTableType.CARDINALITY.getName());
            if (refTableCardinality == null) {
              continue;
            }
            RefTables entryCardinality = refTablesRepository.findOne(tenant.getId(), refTableCardinality.getId(), cardinality);
            if (entryCardinality == null) {
              continue;
            }
            RefTableType refTableStrength = refTableTypeRepository.findOne(tenant.getId(), EnumRefTableType.CARDINALITY_STRENGTH.getName());
            if (refTableStrength == null) {
              continue;
            }
            // Checked for the referenced object null attribute and set the strength accordingly
            RefTables entryCardinalityStrength = refTablesRepository.findOne(tenant.getId(), refTableStrength.getId(), isNullable ? "Aggregation" : "Composition");
            if (entryCardinalityStrength == null) {
              continue;
            }
            Cardinality thisCardinality = cardinalityRepository.findOne(tenant.getId(), property.getId(), cardinalityDataObject.getId());

            if (thisCardinality == null) {
              var request = new CardinalityPostApiRequest();
              request.setIdTenant(tenant.getId());
              request.setIdProperty(property.getId());
              request.setIdDataObject(cardinalityDataObject.getId());
              request.setIdRtCardinality(entryCardinality.getId());
              request.setIdRtCardinalityStrength(entryCardinalityStrength.getId());
              request.setHasReferentialAction(hasReferentialAction);
              request.setCreatedBy(Constant.THIS_PROCESS_CREATED);
              var response = cardinalityRepository.save(request);
              
              if (response.getData() == null) {
                logger.error("Cardinality failed for: {}, {}", name, response.getMeta().getDetail());
                errors++;
              }
              else {
                persisted++;
              }
            }
            else {
              var request = new CardinalityPutApiRequest();
              request.setId(thisCardinality.getId());
              request.setIdRtCardinality(entryCardinality.getId());
              request.setIdRtCardinalityStrength(entryCardinalityStrength.getId());
              request.setHasReferentialAction(hasReferentialAction);
              request.setUpdatedAt(thisCardinality.getUpdatedAt());
              request.setUpdatedBy(Constant.THIS_PROCESS_UPDATED);
              var response = cardinalityRepository.save(request);

              if (response.getData() == null) {
                logger.error("Cardinality failed for: {}, {}", name, response.getMeta().getDetail());
                errors++;
              }
              else {
                persisted++;
              }
            }
          }
        }
      }
    }
    catch (Exception e) {
      logger.error("Error Loading Property {}", e.getMessage());
      throw e;
    }
    finally {
      if (this.getCsvParser() != null) this.getCsvParser().close();
    }
    logger.info("Completed Property Load with {} persisted, {} deleted, and {} errors", persisted, deleted, errors);
  }
}