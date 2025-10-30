package com.norpactech.pf.loader.repository.ex;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.norpactech.nc.api.utils.ApiFindOneRequest;
import com.norpactech.nc.api.utils.ApiResponse;
import com.norpactech.nc.config.load.ConfiguredAPI;
import com.norpactech.nc.config.load.Globals;
import com.norpactech.nc.utils.TextUtils;
import com.norpactech.pf.loader.dto.TenantPostApiRequest;
import com.norpactech.pf.loader.model.Tenant;
import com.norpactech.pf.loader.repository.TenantRepository;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;
/**
 * Special methods for bootstrapping a Tenant. We don't know the Tenant ID
 * because it hasn't been created, so the X-Tenant-ID header isn't sent.
 */
public class TenantRepositoryEx extends TenantRepository {
  
  private static final String RELATIVE_URL = "/bootstrap/tenant";

  @Override
  protected String getRelativeURL() {
    return RELATIVE_URL;
  }

  @Override
  public Tenant findOne(String name) throws Exception {
    var request = new ApiFindOneRequest(Tenant.class, "/tenant/bootstrap", 
        new HashMap<>(Map.of("name", name)));

    var response = this.bootstrap(request);

    if (response.getError() != null) {
      throw new Exception(response.getError().toString());
    }
    if (response.getData() == null) {
      return null;
    }
    return response.readObject(Tenant.class);
  }
  
  public ApiResponse save(TenantPostApiRequest request) throws Exception {
    return bootstrap(toParams(request));
  }  

  public ApiResponse bootstrap(ApiFindOneRequest apiGetRequest) throws Exception {

    String queryString = TextUtils.toQueryString(apiGetRequest.getParams());
    URL url = new URL(Globals.PARETO_API_URL + apiGetRequest.getUri() + queryString.toString());

    okhttp3.Response response = null;
    OkHttpClient client = new OkHttpClient().newBuilder().build();

    okhttp3.Request.Builder requestBuilder = new okhttp3.Request.Builder()
        .url(url)
        .get()
        .addHeader("Accept", "application/json")
        .addHeader("Content-Type", "application/json")
        .addHeader("Authorization", "Bearer " + ConfiguredAPI.jwt);
    
    okhttp3.Request request = requestBuilder.build();
    response = client.newCall(request).execute();       

    int responseCode = response.code();
    if (responseCode > 299) {
      throw new Exception("GET Request Failed: " + response.code());
    }
    return new Gson().fromJson(response.body().string(), ApiResponse.class);
  }   
  
  public ApiResponse bootstrap(Map<String, Object> apiPostRequest) throws Exception {

    URL url = new URL(Globals.PARETO_API_URL + "/tenant/bootstrap");

    OkHttpClient client = new OkHttpClient().newBuilder().build();
    String jsonBody = new Gson().toJson(apiPostRequest);
    RequestBody requestBody = RequestBody.create(jsonBody, MediaType.get("application/json"));

    okhttp3.Request.Builder requestBuilder = new okhttp3.Request.Builder()
        .url(url)
        .post(requestBody)
        .addHeader("Accept", "application/json")
        .addHeader("Content-Type", "application/json")
        .addHeader("Authorization", "Bearer " + ConfiguredAPI.jwt);

    okhttp3.Request request = requestBuilder.build();
    okhttp3.Response response = client.newCall(request).execute();

    int responseCode = response.code();
    if (responseCode > 299) {
        throw new Exception("POST Request Failed: " + response.message());
    }
    return new Gson().fromJson(response.body().string(), ApiResponse.class);
  }    
}