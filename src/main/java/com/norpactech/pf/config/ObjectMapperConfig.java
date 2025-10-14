package com.norpactech.pf.config;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class ObjectMapperConfig {

  public static ObjectMapper createObjectMapper() {
    
    ObjectMapper objectMapper = new ObjectMapper();

    SimpleModule module = new SimpleModule();
    module.addDeserializer(Integer.class, new com.fasterxml.jackson.databind.deser.std.NumberDeserializers.IntegerDeserializer(Integer.class, null));
    objectMapper.registerModule(module);

    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    return objectMapper;
  }
}