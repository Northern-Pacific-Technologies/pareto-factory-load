package com.norpactech.pf.utils;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
public class GenericSelectResponse {

  private Map<String, Object> properties = new HashMap<>();

  public GenericSelectResponse() {}
  
  public GenericSelectResponse(ResultSet rs) {

    if (rs == null) {
      return;
    }
    
    try {
      
      ResultSetMetaData metaData = rs.getMetaData();
      int columnCount = metaData.getColumnCount();

      for (int i = 1; i <= columnCount; i++) {
        String columnName = metaData.getColumnLabel(i);
        Object columnValue = rs.getObject(i);
        properties.put(columnName, columnValue);
      }
    } 
    catch (SQLException e) {
      throw new RuntimeException("Error while copying ResultSet to Map: " + e.getMessage(), e);
    }        
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

}