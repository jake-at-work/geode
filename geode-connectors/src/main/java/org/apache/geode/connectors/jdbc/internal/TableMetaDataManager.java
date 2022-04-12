/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.connectors.jdbc.internal;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.connectors.jdbc.internal.TableMetaData.ColumnMetaData;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.lang.utils.JavaWorkarounds;

/**
 * Given a tableName this manager will determine which column should correspond to the Geode Region
 * key. The current implementation uses a connection to lookup the SQL metadata for the table and
 * find a single column that is a primary key on that table. If the table was configured with more
 * than one column as a primary key or no columns then an exception is thrown. The computation is
 * remembered so that it does not need to be recomputed for the same table name.
 */
public class TableMetaDataManager {
  private static final String DEFAULT_CATALOG = "";
  private static final String DEFAULT_SCHEMA = "";
  private final ConcurrentMap<String, TableMetaDataView> tableToMetaDataMap =
      new ConcurrentHashMap<>();

  public TableMetaDataView getTableMetaDataView(Connection connection,
      RegionMapping regionMapping) {
    return JavaWorkarounds.computeIfAbsent(tableToMetaDataMap, computeTableName(regionMapping),
        k -> computeTableMetaDataView(connection, k, regionMapping));
  }

  /**
   * If the region mapping has been given a table name then return it.
   * Otherwise return the region mapping's region name as the table name.
   */
  String computeTableName(RegionMapping regionMapping) {
    var result = regionMapping.getTableName();
    if (result == null) {
      result = regionMapping.getRegionName();
    }
    return result;
  }

  private TableMetaDataView computeTableMetaDataView(Connection connection,
      String tableName, RegionMapping regionMapping) {
    try {
      var metaData = connection.getMetaData();
      var realCatalogName = getCatalogNameFromMetaData(metaData, regionMapping);
      var realSchemaName = getSchemaNameFromMetaData(metaData, regionMapping, realCatalogName);
      var realTableName =
          getTableNameFromMetaData(metaData, realCatalogName, realSchemaName, tableName);
      var keys = getPrimaryKeyColumnNamesFromMetaData(metaData, realCatalogName,
          realSchemaName, realTableName, regionMapping.getIds());
      var quoteString = metaData.getIdentifierQuoteString();
      var columnMetaDataMap =
          createColumnMetaDataMap(metaData, realCatalogName, realSchemaName, realTableName);
      return new TableMetaData(realCatalogName, realSchemaName, realTableName, keys, quoteString,
          columnMetaDataMap);
    } catch (SQLException e) {
      throw JdbcConnectorException.createException(e);
    }
  }

  String getCatalogNameFromMetaData(DatabaseMetaData metaData, RegionMapping regionMapping)
      throws SQLException {
    var catalogFilter = regionMapping.getCatalog();
    if (catalogFilter == null || catalogFilter.isEmpty()) {
      return DEFAULT_CATALOG;
    }
    try (var catalogs = metaData.getCatalogs()) {
      return findMatchInResultSet(catalogFilter, catalogs, "TABLE_CAT", "catalog");
    }
  }

  String getSchemaNameFromMetaData(DatabaseMetaData metaData, RegionMapping regionMapping,
      String catalogFilter) throws SQLException {
    var schemaFilter = regionMapping.getSchema();
    if (schemaFilter == null || schemaFilter.isEmpty()) {
      if ("PostgreSQL".equals(metaData.getDatabaseProductName())) {
        schemaFilter = "public";
      } else {
        return DEFAULT_SCHEMA;
      }
    }
    try (var schemas = metaData.getSchemas(catalogFilter, "%")) {
      return findMatchInResultSet(schemaFilter, schemas, "TABLE_SCHEM", "schema");
    }
  }

  private String getTableNameFromMetaData(DatabaseMetaData metaData, String catalogFilter,
      String schemaFilter, String tableName) throws SQLException {
    try (var tables = metaData.getTables(catalogFilter, schemaFilter, "%", null)) {
      return findMatchInResultSet(tableName, tables, "TABLE_NAME", "table");
    }
  }

  String findMatchInResultSet(String stringToFind, ResultSet resultSet, String column,
      String description)
      throws SQLException {
    var exactMatches = 0;
    String exactMatch = null;
    var inexactMatches = 0;
    String inexactMatch = null;
    if (resultSet != null) {
      while (resultSet.next()) {
        var name = resultSet.getString(column);
        if (name.equals(stringToFind)) {
          exactMatches++;
          exactMatch = name;
        } else if (name.equalsIgnoreCase(stringToFind)) {
          inexactMatches++;
          inexactMatch = name;
        }
      }
    }
    if (exactMatches == 1) {
      return exactMatch;
    }
    if (inexactMatches > 1 || exactMatches > 1) {
      throw new JdbcConnectorException(
          "Multiple " + description + "s were found that match \"" + stringToFind + '"');
    }
    if (inexactMatches == 1) {
      return inexactMatch;
    }
    throw new JdbcConnectorException(
        "No " + description + " was found that matches \"" + stringToFind + '"');
  }

  private List<String> getPrimaryKeyColumnNamesFromMetaData(DatabaseMetaData metaData,
      String catalogFilter, String schemaFilter, String tableName,
      String ids)
      throws SQLException {
    List<String> keys = new ArrayList<>();

    if (ids != null && !ids.isEmpty()) {
      keys.addAll(Arrays.asList(ids.split(",")));
      for (var key : keys) {
        checkColumnExistsInTable(tableName, metaData, catalogFilter, schemaFilter, key);
      }
    } else {
      try (
          var primaryKeys =
              metaData.getPrimaryKeys(catalogFilter, schemaFilter, tableName)) {
        while (primaryKeys.next()) {
          var key = primaryKeys.getString("COLUMN_NAME");
          keys.add(key);
        }
        if (keys.isEmpty()) {
          throw new JdbcConnectorException(
              "The table " + tableName + " does not have a primary key column.");
        }
      }
    }
    return keys;
  }

  private Map<String, ColumnMetaData> createColumnMetaDataMap(DatabaseMetaData metaData,
      String catalogFilter,
      String schemaFilter, String tableName) throws SQLException {
    Map<String, ColumnMetaData> result = new HashMap<>();
    try (var columnData =
        metaData.getColumns(catalogFilter, schemaFilter, tableName, "%")) {
      while (columnData.next()) {
        var columnName = columnData.getString("COLUMN_NAME");
        var dataType = columnData.getInt("DATA_TYPE");
        var nullableCode = columnData.getInt("NULLABLE");
        var nullable = nullableCode != DatabaseMetaData.columnNoNulls;
        result.put(columnName, new ColumnMetaData(JDBCType.valueOf(dataType), nullable));
      }
    }
    return result;
  }

  private void checkColumnExistsInTable(String tableName, DatabaseMetaData metaData,
      String catalogFilter, String schemaFilter, String columnName) throws SQLException {
    var caseInsensitiveMatches = 0;
    try (var columnData =
        metaData.getColumns(catalogFilter, schemaFilter, tableName, "%")) {
      while (columnData.next()) {
        var realColumnName = columnData.getString("COLUMN_NAME");
        if (columnName.equals(realColumnName)) {
          return;
        } else if (columnName.equalsIgnoreCase(realColumnName)) {
          caseInsensitiveMatches++;
        }
      }
    }
    if (caseInsensitiveMatches > 1) {
      throw new JdbcConnectorException(
          "The table " + tableName + " has more than one column that matches " + columnName);
    } else if (caseInsensitiveMatches == 0) {
      throw new JdbcConnectorException(
          "The table " + tableName + " does not have a column named " + columnName);
    }
  }
}
