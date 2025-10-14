package com.norpactech.pf.loader.vo;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
import java.util.Objects;

public class ForeignKeyVO {
  
  private String table;
  private String foreignKey;
  private String references;
  private String primaryKey;

  public ForeignKeyVO(
      String table,
      String foreignKey,
      String references, 
      String primaryKey) {
    
    this.table = table;
    this.foreignKey = foreignKey;
    this.references = references;
    this.primaryKey = primaryKey;
  }

  @Override
  public String toString() {
    return "ForeignKeyVO [table=" + table + ", foreignKey=" + foreignKey + ", references=" + references
        + ", primaryKey=" + primaryKey + "]";
  }

  @Override
  public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ForeignKeyVO that = (ForeignKeyVO) o;
      return Objects.equals(table, that.table) &&
             Objects.equals(foreignKey, that.foreignKey) &&
             Objects.equals(references, that.references) &&
             Objects.equals(primaryKey, that.primaryKey);
  }

  @Override
  public int hashCode() {
      return Objects.hash(table, foreignKey, references, primaryKey);
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getForeignKey() {
    return foreignKey;
  }

  public void setForeignKey(String foreignKey) {
    this.foreignKey = foreignKey;
  }

  public String getReferences() {
    return references;
  }

  public void setReferences(String references) {
    this.references = references;
  }

  public String getPrimaryKey() {
    return primaryKey;
  }

  public void setPrimaryKey(String primaryKey) {
    this.primaryKey = primaryKey;
  }
}
