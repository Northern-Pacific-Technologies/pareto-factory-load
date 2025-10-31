package com.norpactech.pf.loader.service;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norpactech.nc.api.utils.ApiResponse;
import com.norpactech.nc.utils.TextUtils;
import com.norpactech.pf.loader.dto.PluginPostApiRequest;
import com.norpactech.pf.loader.dto.PluginPutApiRequest;
import com.norpactech.pf.utils.Constant;

public class LoadPlugin extends BaseLoader {

  private static final Logger logger = LoggerFactory.getLogger(LoadPlugin.class);

  public LoadPlugin(String filePath, String fileName) throws Exception {
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
        var contextName = TextUtils.toString(csvRecord.get("context"));
        var name = TextUtils.toString(csvRecord.get("name"));
        var description = TextUtils.toString(csvRecord.get("description"));
        var pluginService = TextUtils.toString(csvRecord.get("plugin_service"));
        
        var context = contextRepository.findOne(contextName);
        if (context == null) {
          logger.error("Context {} not found. Ignoring Plugin {}.", contextName, name);
          continue;
        }
        var plugin = pluginRepository.findOne(context.getId(), name);
        ApiResponse response = null; 
            
        if (action.startsWith("p")) {
          if (plugin == null) {
            var request = new PluginPostApiRequest();
            request.setIdContext(context.getId());
            request.setName(name);
            request.setDescription(description);
            request.setPluginService(pluginService);
            request.setCreatedBy(Constant.THIS_PROCESS_CREATED);
            response = pluginRepository.save(request);
            
            if (response.getData() == null) {
              logger.error("Plugin failed for: {}, {}", name, response.getMeta().getDetail());
              errors++;
            }
            else {
              persisted++;
            }
          }
          else {
            var request = new PluginPutApiRequest();
            request.setId(plugin.getId());
            request.setName(name);
            request.setDescription(description);
            request.setPluginService(pluginService);
            request.setUpdatedAt(plugin.getUpdatedAt());
            request.setUpdatedBy(Constant.THIS_PROCESS_UPDATED);
            response = pluginRepository.save(request);
            
            if (response.getData() == null) {
              logger.error("Plugin failed for: {}, {}", name, response.getMeta().getDetail());
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
      logger.error("Error Loading Schema: {}", e.getMessage());
      throw e;
    }
    finally {
      if (this.getCsvParser() != null) this.getCsvParser().close();
    }
    logger.info("Completed Schema Load with {} persisted, {} deleted, and {} errors", persisted, deleted, errors);
  }
}  