package com.norpactech.pf.loader.service;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Logger;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import com.norpactech.nc.config.load.Globals;
import com.norpactech.nc.config.tenant.TenantContext;
import com.norpactech.pf.loader.model.Tenant;
import com.norpactech.pf.loader.repository.CardinalityRepository;
import com.norpactech.pf.loader.repository.ContextDataTypeRepository;
import com.norpactech.pf.loader.repository.ContextPropertyTypeRepository;
import com.norpactech.pf.loader.repository.ContextRepository;
import com.norpactech.pf.loader.repository.DataIndexPropertyRepository;
import com.norpactech.pf.loader.repository.DataIndexRepository;
import com.norpactech.pf.loader.repository.DataObjectRepository;
import com.norpactech.pf.loader.repository.GenericDataTypeAttributeRepository;
import com.norpactech.pf.loader.repository.GenericDataTypeRepository;
import com.norpactech.pf.loader.repository.PluginRepository;
import com.norpactech.pf.loader.repository.ProjectComponentPropertyRepository;
import com.norpactech.pf.loader.repository.ProjectComponentRepository;
import com.norpactech.pf.loader.repository.ProjectRepository;
import com.norpactech.pf.loader.repository.PropertyRepository;
import com.norpactech.pf.loader.repository.RefTableTypeRepository;
import com.norpactech.pf.loader.repository.RefTablesRepository;
import com.norpactech.pf.loader.repository.SchemaRepository;
import com.norpactech.pf.loader.repository.TenantRepository;
import com.norpactech.pf.loader.repository.TenantUserRepository;
import com.norpactech.pf.loader.repository.UserRepository;
import com.norpactech.pf.loader.repository.ValidationRepository;
import com.norpactech.pf.loader.repository.ex.GenericPropertyTypeRepositoryEx;
import com.norpactech.pf.loader.repository.ex.TenantRepositoryEx;

public abstract class BaseLoader {

  private static final Logger logger = Logger.getLogger(BaseLoader.class.getName());

  protected static final CardinalityRepository cardinalityRepository = new CardinalityRepository();
  protected static final ContextRepository contextRepository = new ContextRepository();
  protected static final ContextDataTypeRepository contextDataTypeRepository = new ContextDataTypeRepository();
  protected static final ContextPropertyTypeRepository contextPropertyTypeRepository = new ContextPropertyTypeRepository();
  protected static final GenericDataTypeAttributeRepository genericDataTypeAttributeRepository = new GenericDataTypeAttributeRepository();
  protected static final GenericDataTypeRepository genericDataTypeRepository = new GenericDataTypeRepository();
  protected static final GenericPropertyTypeRepositoryEx genericPropertyTypeRepository = new GenericPropertyTypeRepositoryEx();
  protected static final DataIndexRepository dataIndexRepository = new DataIndexRepository();
  protected static final DataIndexPropertyRepository dataIndexPropertyRepository = new DataIndexPropertyRepository();
  protected static final PluginRepository pluginRepository = new PluginRepository();
  protected static final ProjectComponentPropertyRepository projectComponentPropertyRepository = new ProjectComponentPropertyRepository();
  protected static final ProjectComponentRepository projectComponentRepository = new ProjectComponentRepository();
  protected static final ProjectRepository projectRepository = new ProjectRepository();
  protected static final RefTablesRepository refTablesRepository = new RefTablesRepository();
  protected static final RefTableTypeRepository refTableTypeRepository = new RefTableTypeRepository();
  protected static final SchemaRepository schemaRepository = new SchemaRepository();
  protected static final TenantRepository tenantRepository = new TenantRepository();
  protected static final TenantRepositoryEx tenantRepositoryEx = new TenantRepositoryEx();
  protected static final TenantUserRepository tenantUserRepository = new TenantUserRepository();
  protected static final UserRepository userRepository = new UserRepository();
  protected static final ValidationRepository validationRepository = new ValidationRepository();
  protected static final DataObjectRepository dataObjectRepository = new DataObjectRepository();
  protected static final PropertyRepository propertyRepository = new PropertyRepository();

  private String filePath;
  private String fileName;
  private CSVParser csvParser;
  private boolean fileExists;
  
  public BaseLoader (String filePath, String fileName) throws Exception {
    this.filePath = filePath;
    this.fileName = fileName;
    
    if (!Files.exists(getFullPath())) {
      this.fileExists = false;
      // The Tenant Context is required if Tenant.csv is not processed
      if (fileName.toUpperCase().equals("TENANT.CSV")) {
        Tenant tenant = tenantRepositoryEx.findOne(Globals.PARETO_API_TENANT_NAME);
        if (tenant != null) {
          TenantContext.setIdTenant(tenant.getId().toString());
        }
        else {
          throw new Exception("Tenant Context could not be set - Tenant.csv file is missing and default tenant not found: " + Globals.PARETO_API_TENANT_NAME);
        }
      }
      logger.info("File does not exist: " + getFullPath() + " - Skipping load");
      return;
    }
    this.fileExists = true;
    Reader reader = Files.newBufferedReader(getFullPath());
    this.csvParser = new CSVParser(reader, CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build());
  }
  
  /**
   * Check if the file exists and is available for loading
   * @return true if file exists and can be loaded, false otherwise
   */
  public boolean isFileAvailable() {
    return fileExists;
  }
  
  public boolean isComment(CSVRecord csvRecord) throws Exception {

    String start = null;
    try {
      start = csvRecord.get(0);

      if(!StringUtils.isNotEmpty(start) ) {
        throw new Exception("Record invalid - col(0) is empty");
      }
      return start.startsWith("//");
    }
    catch (Exception e) {
      throw new Exception("Record col(0) is empty or currupt");
    }
  }
  
  public Path getFullPath() {
    return Paths.get(filePath, fileName);
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public CSVParser getCsvParser() {
    return csvParser;
  }

  public void setCsvParser(CSVParser csvParser) {
    this.csvParser = csvParser;
  }
}