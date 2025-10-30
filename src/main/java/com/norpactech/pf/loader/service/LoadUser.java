package com.norpactech.pf.loader.service;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norpactech.nc.api.utils.ApiResponse;
import com.norpactech.nc.utils.TextUtils;
import com.norpactech.pf.loader.dto.TenantUserPostApiRequest;
import com.norpactech.pf.loader.dto.UserDeleteApiRequest;
import com.norpactech.pf.loader.dto.UserPostApiRequest;
import com.norpactech.pf.loader.dto.UserPutApiRequest;
import com.norpactech.pf.loader.model.TenantUser;
import com.norpactech.pf.utils.Constant;

public class LoadUser extends BaseLoader {

  private static final Logger logger = LoggerFactory.getLogger(LoadUser.class);

  public LoadUser(String filePath, String fileName) throws Exception {
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
        String action = TextUtils.toString(csvRecord.get("action")).toLowerCase();
        String tenantName = TextUtils.toString(csvRecord.get("tenant"));
        String email = TextUtils.toString(csvRecord.get("email"));
        String lastName = TextUtils.toString(csvRecord.get("last_name"));
        String firstName = TextUtils.toString(csvRecord.get("first_name"));
        String phone = TextUtils.toString(csvRecord.get("phone"));
        String street1 = TextUtils.toString(csvRecord.get("street1"));
        String street2 = TextUtils.toString(csvRecord.get("street2"));
        String city = TextUtils.toString(csvRecord.get("city"));
        String state = TextUtils.toString(csvRecord.get("state"));
        String zipCode = TextUtils.toString(csvRecord.get("zip_code"));
        LocalDateTime termsAccepted = LocalDateTime.now(ZoneOffset.UTC);
        
        var tenant = tenantRepository.findOne(tenantName);
        if (tenant == null) {
          logger.error("Tenant {} not found. Ignoring User {}.", tenantName, email);
          continue;
        }
        var user = userRepository.findOne(email);
        ApiResponse response = null; 
        
        if (action.startsWith("p")) {
          if (user == null) {
            var request = new UserPostApiRequest();
            request.setEmail(email);
            request.setOauthIdUser("");
            request.setLastName(lastName);
            request.setFirstName(firstName);
            request.setPhone(phone);
            request.setStreet1(street1);
            request.setStreet2(street2);
            request.setCity(city);
            request.setState(state);
            request.setZipCode(zipCode);
            request.setTermsAccepted(termsAccepted);
            request.setCreatedBy(Constant.THIS_PROCESS_CREATED);
            response = userRepository.save(request);                 
          }
          else {
            var request = new UserPutApiRequest();
            request.setId(user.getId());
            request.setEmail(email);
            request.setOauthIdUser("");
            request.setLastName(lastName);
            request.setFirstName(firstName);
            request.setPhone(phone);
            request.setStreet1(street1);
            request.setStreet2(street2);
            request.setCity(city);
            request.setState(state);
            request.setZipCode(zipCode);
            request.setTermsAccepted(termsAccepted);
            request.setUpdatedAt(user.getUpdatedAt());
            request.setUpdatedBy(Constant.THIS_PROCESS_UPDATED);
            response = userRepository.save(request);                 
          }
          if (response.getData() == null) {
            if (response.getError() != null) {
              logger.error(response.getError().toString());
            }
            else {
              logger.error(this.getClass().getName() + " failed for: " + email + " " + response.getMeta().getDetail());
            }
            errors++;
          }
          else {
            persisted++;
          }          
          user = userRepository.findOne(email);          
          tenant = tenantRepository.findOne(tenantName);
          
          if (user == null) {
            throw new Exception("Null User prior to saving TenantUser: " + email);
          }
          if (tenant == null) {
            throw new Exception("Null Tenant prior to saving TenantUser: " + tenantName);
          }
          TenantUser tenantUser = tenantUserRepository.get(tenant.getId(), user.getId());
          if (tenantUser == null) {
            TenantUserPostApiRequest request = new TenantUserPostApiRequest();
            request.setIdTenant(tenant.getId());
            request.setIdUser(user.getId());
            tenantUserRepository.save(request);
            
            if (response.getData() == null) {
              if (response.getError() != null) {
                logger.error(response.getError().toString());
              }
              else {
                logger.error(this.getClass().getName() + " failed for: " + email + " " + response.getMeta().getDetail());
              }
              errors++;
            }
            else {
              persisted++;          
            }          
          }
        }
        else if (action.startsWith("d") && user != null) {
          var request = new UserDeleteApiRequest();
          request.setId(user.getId());
          request.setUpdatedAt(user.getUpdatedAt());
          request.setUpdatedBy(Constant.THIS_PROCESS_DELETED);
          userRepository.delete(request);
          deleted++;
        }
      }  // for
    }
    catch (Exception e) {
      logger.error("Error Loading User {}", e.getMessage());
      throw e;
    }
    finally {
      if (this.getCsvParser() != null) this.getCsvParser().close();
    }
    logger.info("Completed User Load with {} persisted, {} deleted, and {} errors", persisted, deleted, errors);
  }
}