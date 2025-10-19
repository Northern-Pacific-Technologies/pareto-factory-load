package com.norpactech.pf.loader;
import java.util.UUID;

/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norpactech.pf.config.ConfiguredAPI;
import com.norpactech.pf.config.Globals;
import com.norpactech.pf.loader.service.LoadAll;

public class Application {

  private static final Logger logger = LoggerFactory.getLogger(Application.class);
  
  public static void main(String[] args) throws Exception {

    java.util.Locale.setDefault(java.util.Locale.US);
    java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("UTC"));

    String username   = System.getenv("PARETO_USERNAME");
    String password   = System.getenv("PARETO_PASSWORD");
    String factoryURL = System.getenv("PARETO_FACTORY_URL");
    String apiVersion = System.getenv("PARETO_API_VERSION");
    String tenantUUID = System.getenv("PARETO_TENANT_UUID");
    String filePath   = System.getenv("IMPORT_FILE_PATH");
    
    System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tc %3$s %4$s: %5$s%6$s%n");

    logger.info("Beginning Pareto Loader");
    
    if (StringUtils.isEmpty(username)) {
      logger.error("Null or empty username. Set environment variable: PARETO_USERNAME. Terminating...");
      System.exit(1);
    }
    
    if (StringUtils.isEmpty(password)) {
      logger.error("Null or empty password. Set environment variable: PARETO_PASSWORD. Terminating...");
      System.exit(1);
    }
    
    if (StringUtils.isEmpty(factoryURL)) {
      logger.error("Null or empty Pareto Factory URL. Set environment variable: PARETO_FACTORY_URL. Terminating...");
      System.exit(1);
    }

    if (StringUtils.isEmpty(apiVersion)) {
      logger.error("Null or empty API Version. Set environment variable: PARETO_API_VERSION. Terminating...");
      System.exit(1);
    }    

    if (StringUtils.isEmpty(tenantUUID)) {
      logger.error("Null or empty Tenant ID. Set environment variable: PARETO_TENANT_UUID. Terminating...");
      System.exit(1);
    }    
    
    if (StringUtils.isEmpty(filePath)) {
      logger.error("Null or empty Import File Path. Set environment variable: IMPORT_FILE_PATH. Terminating...");
      System.exit(1);
    }    
    
    try {
      Globals.setIdTenant(UUID.fromString(tenantUUID));
      ConfiguredAPI.configure(factoryURL, apiVersion, username, password);
      LoadAll.load(filePath);
      System.exit(0);
    }
    catch (Exception e) {
      logger.error("Pareto Factory Loader Terminated Unexpectedly: " + e.getMessage());
      System.exit(1);
    }
  }
}