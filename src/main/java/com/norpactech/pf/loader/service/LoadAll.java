package com.norpactech.pf.loader.service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadAll {

  private static final Logger logger = LoggerFactory.getLogger(LoadAll.class);
  
  public static void load(String filePath) throws Exception {
    
    logger.info("Beginning Load All from: {}", filePath);

    new LoadTenant(filePath, "Tenant.csv").load();
    
    new LoadUser(filePath, "User.csv").load(); // includes TenantUser  **** Revisit ****
    
    new LoadSchema(filePath, "Schema.csv").load();
    new LoadContext(filePath, "Context.csv").load();
    new LoadRefTableType(filePath, "RefTableType.csv").load();
    new LoadRefTables(filePath, "RefTables.csv").load();
    new LoadValidation(filePath, "Validation.csv").load();    
    new LoadGenericDataType(filePath, "GenericDataType.csv").load();
    new LoadGenericDataTypeAttribute(filePath, "GenericDataTypeAttribute.csv").load();
    new LoadGenericPropertyType(filePath, "GenericPropertyType.csv").load();
    new LoadContextDataType(filePath, "ContextDataType.csv").load();
    new LoadContextPropertyType(filePath, "ContextPropertyType.csv").load();
    new LoadDataObject(filePath, "DataObject.csv").load();
    new LoadProperty(filePath, "Property.csv").load();
    new LoadDataIndex(filePath, "DataIndex.csv").load();
// Data Index Property
    
// Plugin
    new LoadProject(filePath, "Project.csv").load();
    new LoadProjectComponent(filePath, "ProjectComponent.csv").load();
    new LoadProjectComponentProperty(filePath, "ProjectComponentProperty.csv").load();
    new LoadTenant(filePath, "Tenant.csv").load();  // Repeated
    
    logger.info("Completed Load All");
  }
}  