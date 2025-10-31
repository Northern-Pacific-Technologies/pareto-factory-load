package com.norpactech.pf.loader.repository;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.norpactech.nc.api.utils.ApiGetRequest;
import com.norpactech.nc.api.utils.ApiResponse;
import com.norpactech.nc.config.json.GsonConfig;
import com.norpactech.nc.config.load.ConfiguredAPI;
import com.norpactech.nc.config.load.Globals;
import com.norpactech.nc.utils.TextUtils;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;

public abstract class ParetoNativeRepository<T> {

  private static final Gson gson = GsonConfig.getInstance();
  protected abstract String getRelativeURL();

  public T findOne(Class<T> entityType, Map<String, Object> queryParams) throws Exception {
    
    var toStringMap = new HashMap<String, String>();
    for (String key : queryParams.keySet()) {
      Object val = queryParams.get(key);
      if (val != null) {
        toStringMap.put(key, val.toString());
      }
    }
    var request = new ApiGetRequest(entityType, getRelativeURL(), toStringMap);
    var response = get(request);

    if (response.getError() != null) {
      throw new Exception(response.getError().toString());
    }
    if (response.getData() == null) {
      return null;
    }
    return response.readObject(entityType);
  }
  
  public List<T> find(Class<T> entityType, Map<String, Object> queryParams) throws Exception {

    var toStringMap = new HashMap<String, String>();
    for (String key : queryParams.keySet()) {
      Object val = queryParams.get(key);
      if (val != null) {
        toStringMap.put(key, val.toString());
      }
    }
    ApiGetRequest request = new ApiGetRequest(entityType, getRelativeURL(), toStringMap);
    request.setUri(getRelativeURL() + "/find");
    
    ApiResponse response = get(request);

    if (response.getError() != null) {
      throw new Exception(response.getError().toString());
    }
    if (response.getData() == null) {
      return new ArrayList<>();
    }
    return response.readList(entityType);
  }
  
  public T post(Class<T> entityType, Map<String, Object> params) throws Exception {
    

    return null;
  }  
  
  public ApiResponse get(ApiGetRequest apiGetRequest) throws Exception {

    String version = ConfiguredAPI.apiVersion == null ? "" : "/" + ConfiguredAPI.apiVersion;
    String queryString = TextUtils.toQueryString(apiGetRequest.getParams());
    URL url = new URL(ConfiguredAPI.host + version + apiGetRequest.getUri() + queryString.toString());

    okhttp3.Response response = null;
    OkHttpClient client = new OkHttpClient().newBuilder().build();

    okhttp3.Request.Builder requestBuilder = new okhttp3.Request.Builder()
        .url(url)
        .get()
        .addHeader("Accept", "application/json")
        .addHeader("Content-Type", "application/json")
        .addHeader("Authorization", "Bearer " + ConfiguredAPI.jwt)
        .addHeader("X-Tenant-ID", Globals.getIdTenant().toString());
    
    okhttp3.Request request = requestBuilder.build();
    response = client.newCall(request).execute();       

    int responseCode = response.code();
    if (responseCode > 299) {
      throw new Exception("GET Request Failed: " + response.message());
    }
    return gson.fromJson(response.body().string(), ApiResponse.class);
  } 
  
  public ApiResponse post(Map<String, Object> apiPostRequest) throws Exception {

    String version = ConfiguredAPI.apiVersion == null ? "" : "/" + ConfiguredAPI.apiVersion;
    URL url = new URL(ConfiguredAPI.host + version + getRelativeURL());

    OkHttpClient client = new OkHttpClient().newBuilder().build();
    String jsonBody = gson.toJson(apiPostRequest);
    RequestBody requestBody = RequestBody.create(jsonBody, MediaType.get("application/json"));

    okhttp3.Request.Builder requestBuilder = new okhttp3.Request.Builder()
        .url(url)
        .post(requestBody)
        .addHeader("Accept", "application/json")
        .addHeader("Content-Type", "application/json")
        .addHeader("Authorization", "Bearer " + ConfiguredAPI.jwt)
        .addHeader("X-Tenant-ID", Globals.getIdTenant().toString());

    okhttp3.Request request = requestBuilder.build();
    okhttp3.Response response = client.newCall(request).execute();

    int responseCode = response.code();
    if (responseCode > 299) {
        throw new Exception("POST Request Failed: " + response.message());
    }
    return gson.fromJson(response.body().string(), ApiResponse.class);
  }
  
  public ApiResponse put(Map<String, Object> apiPutRequest) throws Exception {

    String version = ConfiguredAPI.apiVersion == null ? "" : "/" + ConfiguredAPI.apiVersion;
    URL url = new URL(ConfiguredAPI.host + version + getRelativeURL());

    OkHttpClient client = new OkHttpClient().newBuilder().build();
    String jsonBody = gson.toJson(apiPutRequest);
    RequestBody requestBody = RequestBody.create(jsonBody, MediaType.get("application/json"));

    okhttp3.Request.Builder requestBuilder = new okhttp3.Request.Builder()
        .url(url)
        .put(requestBody)
        .addHeader("Accept", "application/json")
        .addHeader("Content-Type", "application/json")
        .addHeader("Authorization", "Bearer " + ConfiguredAPI.jwt)
        .addHeader("X-Tenant-ID", Globals.getIdTenant().toString());

    okhttp3.Request request = requestBuilder.build();
    okhttp3.Response response = client.newCall(request).execute();

    int responseCode = response.code();
    if (responseCode > 299) {
        throw new Exception("POST Request Failed: " + response.message());
    }
    return gson.fromJson(response.body().string(), ApiResponse.class);
  }
  
  public ApiResponse delete(Map<String, Object> apiDeleteRequest) throws Exception {

    String version = ConfiguredAPI.apiVersion == null ? "" : "/" + ConfiguredAPI.apiVersion;
    URL url = new URL(ConfiguredAPI.host + version + getRelativeURL());

    OkHttpClient client = new OkHttpClient().newBuilder().build();
    String jsonBody = gson.toJson(apiDeleteRequest);
    RequestBody requestBody = RequestBody.create(jsonBody, MediaType.get("application/json"));

    okhttp3.Request.Builder requestBuilder = new okhttp3.Request.Builder()
        .url(url)
        .delete(requestBody)
        .addHeader("Accept", "application/json")
        .addHeader("Content-Type", "application/json")
        .addHeader("Authorization", "Bearer " + ConfiguredAPI.jwt)
        .addHeader("X-Tenant-ID", Globals.getIdTenant().toString());

    okhttp3.Request request = requestBuilder.build();
    okhttp3.Response response = client.newCall(request).execute();

    int responseCode = response.code();
    if (responseCode > 299) {
        throw new Exception("POST Request Failed: " + response.message());
    }
    return gson.fromJson(response.body().string(), ApiResponse.class);
  }
  
  public Map<String, Object> toParams(Object request) throws IllegalAccessException {
    
    // Use Gson to serialize the object, then deserialize back to Map
    // This ensures all custom serializers (like Timestamp -> ISO 8601) are applied
    String json = gson.toJson(request);
    @SuppressWarnings("unchecked")
    Map<String, Object> map = gson.fromJson(json, Map.class);
    return map;
  }
}