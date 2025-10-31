package com.norpactech.pf.loader.service;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norpactech.nc.utils.TextUtils;
import com.norpactech.pf.loader.dto.ContextPostApiRequest;
import com.norpactech.pf.loader.dto.ContextPutApiRequest;
import com.norpactech.pf.utils.Constant;

public class LoadContext extends BaseLoader {

  private static final Logger logger = LoggerFactory.getLogger(LoadContext.class);

  public LoadContext(String filePath, String fileName) throws Exception {
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
        var name = TextUtils.toString(csvRecord.get("name"));
        var description = TextUtils.toString(csvRecord.get("description"));

        var context = contextRepository.findOne(name);
        
        if (action.startsWith("p")) {
          if (context == null) {
            var request = new ContextPostApiRequest();
            request.setName(name);
            request.setDescription(description);
            request.setCreatedBy(Constant.THIS_PROCESS_CREATED);
            var response = contextRepository.save(request);
            
            if (response.getData() == null) {
              logger.error("Context failed for: " + name + " " + response.getMeta().getDetail());
              errors++;
            }
            else {
              persisted++;
            }
          }
          else {
            var request = new ContextPutApiRequest();
            request.setId(context.getId());
            request.setName(name);
            request.setDescription(description);
            request.setUpdatedAt(context.getUpdatedAt());
            request.setUpdatedBy(Constant.THIS_PROCESS_UPDATED);
            var response = contextRepository.save(request);

            if (response.getData() == null) {
              logger.error("Context failed for: " + name + " " + response.getMeta().getDetail());
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
      logger.error("Error Loading Context: {}", e.getMessage());
      throw e;
    }
    finally {
      if (this.getCsvParser() != null) this.getCsvParser().close();
    }
    logger.info("Completed Context Load with {} persisted, {} deleted, and {} errors", persisted, deleted, errors);
  }
}  