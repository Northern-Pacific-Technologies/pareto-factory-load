package com.norpactech.pf.loader.enums;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
/**
 * Required Fixed Project Component Names
 */
public enum EnumProjectComponent {

  DATABASE_TABLES("database_tables"),
  DATABASE_VALIDATIONS("database_validations"),
  DATABASE_INSERT("database_insert"),
  DATABASE_UPDATE("database_update"),
  DATABASE_DELETE("database_delete"),
  DATABASE_ACTIVE("database_active"),  
  
  JAVA_API_MODEL("java_api_model"),
  JAVA_API_CONTROLLER("java_api_controller"),
  JAVA_API_ACTIVE_REQUEST("java_api_active_request"),
  JAVA_API_DELETE_REQUEST("java_api_delete_request"),
  JAVA_API_POST_REQUEST("java_api_post_request"),
  JAVA_API_PUT_REQUEST("java_api_put_request"),

  ANGULAR_MODEL("angular_model"),
  ANGULAR_SERVICE("angular_service"),
  ANGULAR_ACTIVE_DTO_CLASS("angular_active_dto_class"),
  ANGULAR_DELETE_DTO_CLASS("angular_delete_dto_class"),
  ANGULAR_POST_DTO_CLASS("angular_post_dto_class"),
  ANGULAR_PUT_DTO_CLASS("angular_put_dto_class");
  
  private String name;

  private EnumProjectComponent(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public static EnumProjectComponent fromName(String name) {
    for (EnumProjectComponent type : EnumProjectComponent.values()) {
      if (type.getName().equals(name)) {
        return type;
      }
    }
    return null;
  }
}
