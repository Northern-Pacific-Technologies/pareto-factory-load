package com.norpactech.pf.loader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norpactech.nc.config.load.Globals;
import com.norpactech.nc.config.load.ConfiguredAPI;
import com.norpactech.pf.loader.service.LoadAll;

public class Application {

  private static final Logger logger = LoggerFactory.getLogger(Application.class);
  
  public static void main(String[] args) throws Exception {

    java.util.Locale.setDefault(java.util.Locale.US);
    java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("UTC"));
    System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tc %3$s %4$s: %5$s%6$s%n");

    logger.info("Beginning Pareto Loader");
    
    try {
      Globals.validateApiConfiguration();
      Globals.validateLoaderConfiguration();
      
      ConfiguredAPI.configure(Globals.PARETO_API_URL, Globals.PARETO_API_VERSION, Globals.PARETO_API_USERNAME, Globals.PARETO_API_PASSWORD);
      LoadAll.load(Globals.IMPORT_DATA_DIRECTORY);
      System.exit(0);
    }
    catch (Exception e) {
      logger.error("Pareto Factory Loader Terminated Unexpectedly: " + e.getMessage());
      System.exit(1);
    }
  }
}