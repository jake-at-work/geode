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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;

public class TableMetaDataManagerTest {
  private static final String TABLE_NAME = "testTable";
  private static final String KEY_COLUMN = "keyColumn";
  private static final String KEY_COLUMN2 = "keyColumn2";

  private TableMetaDataManager tableMetaDataManager;
  private Connection connection;
  DatabaseMetaData databaseMetaData;
  ResultSet tablesResultSet;
  ResultSet primaryKeysResultSet;
  ResultSet columnResultSet;
  RegionMapping regionMapping;

  @Before
  public void setup() throws Exception {
    tableMetaDataManager = new TableMetaDataManager();
    connection = mock(Connection.class);
    databaseMetaData = mock(DatabaseMetaData.class);
    when(connection.getMetaData()).thenReturn(databaseMetaData);
    tablesResultSet = mock(ResultSet.class);
    when(databaseMetaData.getTables(any(), any(), any(), any())).thenReturn(tablesResultSet);
    primaryKeysResultSet = mock(ResultSet.class);
    when(databaseMetaData.getPrimaryKeys(any(), any(), anyString()))
        .thenReturn(primaryKeysResultSet);
    columnResultSet = mock(ResultSet.class);
    when(databaseMetaData.getColumns(any(), any(), eq(TABLE_NAME), any()))
        .thenReturn(columnResultSet);
    regionMapping = mock(RegionMapping.class);
    when(regionMapping.getTableName()).thenReturn(TABLE_NAME);
  }

  @Test
  public void returnsSinglePrimaryKeyColumnName() throws Exception {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);
    when(regionMapping.getIds()).thenReturn("");

    var data = tableMetaDataManager.getTableMetaDataView(connection, regionMapping);

    assertThat(data.getKeyColumnNames()).isEqualTo(Arrays.asList(KEY_COLUMN));
    verify(connection).getMetaData();
  }

  @Test
  public void returnsCompositePrimaryKeyColumnNames() throws Exception {
    setupCompositePrimaryKeysMetaData();
    when(regionMapping.getIds()).thenReturn("");

    var data = tableMetaDataManager.getTableMetaDataView(connection, regionMapping);

    assertThat(data.getKeyColumnNames()).isEqualTo(Arrays.asList(KEY_COLUMN, KEY_COLUMN2));
    verify(connection).getMetaData();
    verify(databaseMetaData).getTables("", "", "%", null);
  }

  @Test
  public void verifyPostgreUsesPublicSchemaByDefault() throws Exception {
    setupCompositePrimaryKeysMetaData();
    when(regionMapping.getIds()).thenReturn("");
    var schemas = mock(ResultSet.class);
    when(schemas.next()).thenReturn(true).thenReturn(false);
    when(schemas.getString("TABLE_SCHEM")).thenReturn("PUBLIC");
    when(databaseMetaData.getSchemas(any(), any())).thenReturn(schemas);
    when(databaseMetaData.getDatabaseProductName()).thenReturn("PostgreSQL");

    var data = tableMetaDataManager.getTableMetaDataView(connection, regionMapping);

    assertThat(data.getKeyColumnNames()).isEqualTo(Arrays.asList(KEY_COLUMN, KEY_COLUMN2));
    verify(connection).getMetaData();
    verify(databaseMetaData).getTables("", "PUBLIC", "%", null);
  }

  @Test
  public void givenNoColumnsAndNonNullIdsThenExpectException() throws Exception {
    setupTableMetaData();
    when(columnResultSet.next()).thenReturn(false);
    when(regionMapping.getIds()).thenReturn("nonExistentId");

    assertThatThrownBy(
        () -> tableMetaDataManager.getTableMetaDataView(connection, regionMapping))
            .isInstanceOf(JdbcConnectorException.class)
            .hasMessageContaining("The table testTable does not have a column named nonExistentId");
  }

  @Test
  public void givenOneColumnAndNonNullIdsThatDoesNotMatchThenExpectException() throws Exception {
    setupTableMetaData();
    when(columnResultSet.next()).thenReturn(true).thenReturn(false);
    when(columnResultSet.getString("COLUMN_NAME")).thenReturn("existingColumn");
    when(regionMapping.getIds()).thenReturn("nonExistentId");

    assertThatThrownBy(
        () -> tableMetaDataManager.getTableMetaDataView(connection, regionMapping))
            .isInstanceOf(JdbcConnectorException.class)
            .hasMessageContaining("The table testTable does not have a column named nonExistentId");
  }

  @Test
  public void givenTwoColumnsAndNonNullIdsThatDoesNotExactlyMatchThenExpectException()
      throws Exception {
    setupTableMetaData();
    when(columnResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(columnResultSet.getString("COLUMN_NAME")).thenReturn("nonexistentid")
        .thenReturn("NONEXISTENTID");
    when(regionMapping.getIds()).thenReturn("nonExistentId");

    assertThatThrownBy(
        () -> tableMetaDataManager.getTableMetaDataView(connection, regionMapping))
            .isInstanceOf(JdbcConnectorException.class).hasMessageContaining(
                "The table testTable has more than one column that matches nonExistentId");
  }

  @Test
  public void givenThreeColumnsAndNonNullIdsThatDoesExactlyMatchThenKeyColumnNameIsReturned()
      throws Exception {
    setupTableMetaData();
    when(columnResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true)
        .thenReturn(false);
    when(columnResultSet.getString("COLUMN_NAME")).thenReturn("existentid").thenReturn("EXISTENTID")
        .thenReturn("ExistentId");
    when(regionMapping.getIds()).thenReturn("ExistentId");

    var data =
        tableMetaDataManager.getTableMetaDataView(connection, regionMapping);

    assertThat(data.getKeyColumnNames()).isEqualTo(Arrays.asList("ExistentId"));
  }

  @Test
  public void givenFourColumnsAndCompositeIdsThenOnlyKeyColumnNamesAreReturned()
      throws Exception {
    setupTableMetaData();
    when(columnResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true)
        .thenReturn(true).thenReturn(false);
    when(columnResultSet.getString("COLUMN_NAME")).thenReturn("LeadingNonKeyColumn")
        .thenReturn(KEY_COLUMN).thenReturn(KEY_COLUMN2)
        .thenReturn("NonKeyColumn");
    when(regionMapping.getIds()).thenReturn(KEY_COLUMN + "," + KEY_COLUMN2);

    var data =
        tableMetaDataManager.getTableMetaDataView(connection, regionMapping);

    assertThat(data.getKeyColumnNames()).isEqualTo(Arrays.asList(KEY_COLUMN, KEY_COLUMN2));
  }

  @Test
  public void givenColumnAndNonNullIdsThatDoesInexactlyMatchThenKeyColumnNameIsReturned()
      throws Exception {
    setupTableMetaData();
    when(columnResultSet.next()).thenReturn(true).thenReturn(false);
    when(columnResultSet.getString("COLUMN_NAME")).thenReturn("existentid");
    when(regionMapping.getIds()).thenReturn("ExistentId");

    var data =
        tableMetaDataManager.getTableMetaDataView(connection, regionMapping);

    assertThat(data.getKeyColumnNames()).isEqualTo(Arrays.asList("ExistentId"));
  }

  @Test
  public void returnsDefaultQuoteString() throws Exception {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);

    var data =
        tableMetaDataManager.getTableMetaDataView(connection, regionMapping);

    assertThat(data.getIdentifierQuoteString()).isEqualTo("");
    verify(connection).getMetaData();
  }

  @Test
  public void returnsQuoteStringFromMetaData() throws Exception {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);
    var expectedQuoteString = "123";
    when(databaseMetaData.getIdentifierQuoteString()).thenReturn(expectedQuoteString);

    var data =
        tableMetaDataManager.getTableMetaDataView(connection, regionMapping);

    assertThat(data.getIdentifierQuoteString()).isEqualTo(expectedQuoteString);
    verify(connection).getMetaData();
  }

  @Test
  public void secondCallDoesNotUseMetaData() throws Exception {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);

    tableMetaDataManager.getTableMetaDataView(connection, regionMapping);
    tableMetaDataManager.getTableMetaDataView(connection, regionMapping);
    verify(connection).getMetaData();
  }

  @Test
  public void throwsExceptionWhenFailsToGetTableMetadata() throws Exception {
    var cause = new SQLException("sql message");
    when(connection.getMetaData()).thenThrow(cause);

    assertThatThrownBy(
        () -> tableMetaDataManager.getTableMetaDataView(connection, regionMapping))
            .isInstanceOf(JdbcConnectorException.class).hasMessageContaining("sql message");
  }

  @Test
  public void throwsExceptionWhenDesiredTableNotFound() throws Exception {
    when(tablesResultSet.next()).thenReturn(true).thenReturn(false);
    when(tablesResultSet.getString("TABLE_NAME")).thenReturn("otherTable");

    assertThatThrownBy(
        () -> tableMetaDataManager.getTableMetaDataView(connection, regionMapping))
            .isInstanceOf(JdbcConnectorException.class)
            .hasMessage("No table was found that matches \"" + TABLE_NAME + '"');
  }

  @Test
  public void returnsExactMatchTableNameWhenTwoTablesHasCaseInsensitiveSameName() throws Exception {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);
    when(tablesResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(tablesResultSet.getString("TABLE_NAME")).thenReturn(TABLE_NAME.toUpperCase())
        .thenReturn(TABLE_NAME);

    var data =
        tableMetaDataManager.getTableMetaDataView(connection, regionMapping);

    assertThat(data.getQuotedTablePath()).isEqualTo(TABLE_NAME);
  }

  @Test
  public void returnsQuotedTableNameWhenMetaDataHasQuoteId() throws Exception {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);
    when(tablesResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(tablesResultSet.getString("TABLE_NAME")).thenReturn(TABLE_NAME.toUpperCase())
        .thenReturn(TABLE_NAME);
    var QUOTE = "@@";
    when(databaseMetaData.getIdentifierQuoteString()).thenReturn(QUOTE);

    var data =
        tableMetaDataManager.getTableMetaDataView(connection, regionMapping);

    assertThat(data.getQuotedTablePath()).isEqualTo(QUOTE + TABLE_NAME + QUOTE);
  }

  @Test
  public void returnsMatchTableNameWhenMetaDataHasOneInexactMatch() throws Exception {
    setupPrimaryKeysMetaData();
    when(databaseMetaData.getColumns(any(), any(), eq(TABLE_NAME.toUpperCase()), any()))
        .thenReturn(columnResultSet);
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);
    when(tablesResultSet.next()).thenReturn(true).thenReturn(false);
    when(tablesResultSet.getString("TABLE_NAME")).thenReturn(TABLE_NAME.toUpperCase());

    var data =
        tableMetaDataManager.getTableMetaDataView(connection, regionMapping);

    assertThat(data.getQuotedTablePath()).isEqualTo(TABLE_NAME.toUpperCase());
  }

  @Test
  public void throwsExceptionWhenTwoTablesHasCaseInsensitiveSameName() throws Exception {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);
    when(tablesResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);

    when(tablesResultSet.getString("TABLE_NAME")).thenReturn(TABLE_NAME.toLowerCase())
        .thenReturn(TABLE_NAME.toUpperCase());

    assertThatThrownBy(
        () -> tableMetaDataManager.getTableMetaDataView(connection, regionMapping))
            .isInstanceOf(JdbcConnectorException.class)
            .hasMessage("Multiple tables were found that match \"" + TABLE_NAME + '"');
  }

  @Test
  public void throwsExceptionWhenTwoTablesHaveExactSameName() throws Exception {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);
    when(tablesResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);

    when(tablesResultSet.getString("TABLE_NAME")).thenReturn(TABLE_NAME).thenReturn(TABLE_NAME);

    assertThatThrownBy(
        () -> tableMetaDataManager.getTableMetaDataView(connection, regionMapping))
            .isInstanceOf(JdbcConnectorException.class)
            .hasMessage("Multiple tables were found that match \"" + TABLE_NAME + '"');
  }

  @Test
  public void throwsExceptionWhenNoPrimaryKeyInTable() throws Exception {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(false);

    assertThatThrownBy(
        () -> tableMetaDataManager.getTableMetaDataView(connection, regionMapping))
            .isInstanceOf(JdbcConnectorException.class)
            .hasMessage("The table " + TABLE_NAME + " does not have a primary key column.");
  }

  @Test
  public void unknownColumnsDataTypeIsZero() throws SQLException {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);

    var data =
        tableMetaDataManager.getTableMetaDataView(connection, regionMapping);
    var dataType = data.getColumnDataType("unknownColumn");

    assertThat(dataType).isEqualTo(JDBCType.NULL);
  }

  @Test
  public void validateExpectedDataTypes() throws SQLException {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);
    when(columnResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    var columnName1 = "columnName1";
    var columnDataType1 = 1;
    var columnName2 = "columnName2";
    var columnDataType2 = 2;
    when(columnResultSet.getString("COLUMN_NAME")).thenReturn(columnName1).thenReturn(columnName2);
    when(columnResultSet.getInt("DATA_TYPE")).thenReturn(columnDataType1)
        .thenReturn(columnDataType2);

    var data =
        tableMetaDataManager.getTableMetaDataView(connection, regionMapping);
    var dataType1 = data.getColumnDataType(columnName1);
    var dataType2 = data.getColumnDataType(columnName2);

    assertThat(dataType1.getVendorTypeNumber()).isEqualTo(columnDataType1);
    assertThat(dataType2.getVendorTypeNumber()).isEqualTo(columnDataType2);
    verify(primaryKeysResultSet).close();
    verify(columnResultSet).close();
  }

  @Test
  public void validateExpectedColumnNames() throws SQLException {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);
    when(columnResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    var columnName1 = "columnName1";
    var columnDataType1 = 1;
    var columnName2 = "columnName2";
    var columnDataType2 = 2;
    when(columnResultSet.getString("COLUMN_NAME")).thenReturn(columnName1).thenReturn(columnName2);
    when(columnResultSet.getInt("DATA_TYPE")).thenReturn(columnDataType1)
        .thenReturn(columnDataType2);
    Set<String> expectedColumnNames = new HashSet<>(Arrays.asList(columnName1, columnName2));

    var data =
        tableMetaDataManager.getTableMetaDataView(connection, regionMapping);
    var columnNames = data.getColumnNames();

    assertThat(columnNames).isEqualTo(expectedColumnNames);
  }

  @Test
  public void validateThatCloseOnPrimaryKeysResultSetIsCalledByGetTableMetaDataView()
      throws SQLException {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);

    var data =
        tableMetaDataManager.getTableMetaDataView(connection, regionMapping);

    verify(primaryKeysResultSet).close();
  }

  @Test
  public void validateThatCloseOnColumnResultSetIsCalledByGetTableMetaDataView()
      throws SQLException {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);

    var data =
        tableMetaDataManager.getTableMetaDataView(connection, regionMapping);

    verify(columnResultSet).close();
  }

  @Test
  public void validateTableNameIsSetByGetTableMetaDataView() throws SQLException {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);

    var data =
        tableMetaDataManager.getTableMetaDataView(connection, regionMapping);

    assertThat(data.getQuotedTablePath()).isEqualTo(TABLE_NAME);
  }

  private void setupPrimaryKeysMetaData() throws SQLException {
    when(primaryKeysResultSet.getString("COLUMN_NAME")).thenReturn(KEY_COLUMN);
    setupTableMetaData();
  }

  private void setupCompositePrimaryKeysMetaData() throws SQLException {
    when(primaryKeysResultSet.getString("COLUMN_NAME")).thenReturn(KEY_COLUMN)
        .thenReturn(KEY_COLUMN2);
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    setupTableMetaData();
  }

  private void setupTableMetaData() throws SQLException {
    when(tablesResultSet.next()).thenReturn(true).thenReturn(false);
    when(tablesResultSet.getString("TABLE_NAME")).thenReturn(TABLE_NAME);
  }

  @Test
  public void computeTableNameGivenRegionMappingTableNameReturnsIt() {
    when(regionMapping.getTableName()).thenReturn("myTableName");

    var result = tableMetaDataManager.computeTableName(regionMapping);

    assertThat(result).isEqualTo("myTableName");
  }

  @Test
  public void computeTableNameGivenRegionMappingRegionNameReturnsItIfTableNameIsNull() {
    when(regionMapping.getTableName()).thenReturn(null);
    when(regionMapping.getRegionName()).thenReturn("myRegionName");

    var result = tableMetaDataManager.computeTableName(regionMapping);

    assertThat(result).isEqualTo("myRegionName");
  }

  @Test
  public void getCatalogNameFromMetaDataGivenNullCatalogReturnsEmptyString() throws SQLException {
    when(regionMapping.getCatalog()).thenReturn(null);

    var result = tableMetaDataManager.getCatalogNameFromMetaData(null, regionMapping);

    assertThat(result).isEqualTo("");
  }

  @Test
  public void getCatalogNameFromMetaDataGivenEmptyCatalogReturnsEmptyString() throws SQLException {
    when(regionMapping.getCatalog()).thenReturn("");

    var result = tableMetaDataManager.getCatalogNameFromMetaData(null, regionMapping);

    assertThat(result).isEqualTo("");
  }

  @Test
  public void getCatalogNameFromMetaDataGivenCatalogReturnIt() throws SQLException {
    var myCatalog = "myCatalog";
    when(regionMapping.getCatalog()).thenReturn(myCatalog);
    var catalogsResultSet = mock(ResultSet.class);
    when(catalogsResultSet.next()).thenReturn(true).thenReturn(false);
    when(catalogsResultSet.getString("TABLE_CAT")).thenReturn(myCatalog);
    when(databaseMetaData.getCatalogs()).thenReturn(catalogsResultSet);

    var result =
        tableMetaDataManager.getCatalogNameFromMetaData(databaseMetaData, regionMapping);

    assertThat(result).isEqualTo(myCatalog);
  }

  @Test
  public void getSchemaNameFromMetaDataGivenNullSchemaReturnsEmptyString() throws SQLException {
    when(regionMapping.getSchema()).thenReturn(null);

    var result =
        tableMetaDataManager.getSchemaNameFromMetaData(databaseMetaData, regionMapping, null);

    assertThat(result).isEqualTo("");
  }

  @Test
  public void getSchemaNameFromMetaDataGivenEmptySchemaReturnsEmptyString() throws SQLException {
    when(regionMapping.getSchema()).thenReturn("");

    var result =
        tableMetaDataManager.getSchemaNameFromMetaData(databaseMetaData, regionMapping, null);

    assertThat(result).isEqualTo("");
  }

  @Test
  public void getSchemaNameFromMetaDataGivenSchemaReturnsIt() throws SQLException {
    var mySchema = "mySchema";
    when(regionMapping.getSchema()).thenReturn(mySchema);
    var schemasResultSet = mock(ResultSet.class);
    when(schemasResultSet.next()).thenReturn(true).thenReturn(false);
    when(schemasResultSet.getString("TABLE_SCHEM")).thenReturn(mySchema);
    var catalogFilter = "myCatalogFilter";
    when(databaseMetaData.getSchemas(catalogFilter, "%")).thenReturn(schemasResultSet);

    var result = tableMetaDataManager.getSchemaNameFromMetaData(databaseMetaData, regionMapping,
        catalogFilter);

    assertThat(result).isEqualTo(mySchema);
  }

  @Test
  public void getSchemaNameFromMetaDataGivenNullSchemaOnPostgresReturnsPublic()
      throws SQLException {
    var defaultPostgresSchema = "public";
    when(regionMapping.getSchema()).thenReturn(null);
    var schemasResultSet = mock(ResultSet.class);
    when(schemasResultSet.next()).thenReturn(true).thenReturn(false);
    when(schemasResultSet.getString("TABLE_SCHEM")).thenReturn(defaultPostgresSchema);
    var catalogFilter = "myCatalogFilter";
    when(databaseMetaData.getSchemas(catalogFilter, "%")).thenReturn(schemasResultSet);
    when(databaseMetaData.getDatabaseProductName()).thenReturn("PostgreSQL");

    var result = tableMetaDataManager.getSchemaNameFromMetaData(databaseMetaData, regionMapping,
        catalogFilter);

    assertThat(result).isEqualTo(defaultPostgresSchema);
  }

  @Test
  public void findMatchInResultSetGivenEmptyResultSetThrows() throws SQLException {
    var stringToFind = "stringToFind";
    var resultSet = mock(ResultSet.class);
    var column = "column";
    var description = "description";

    assertThatThrownBy(
        () -> tableMetaDataManager.findMatchInResultSet(stringToFind, resultSet, column,
            description))
                .isInstanceOf(JdbcConnectorException.class)
                .hasMessageContaining(
                    "No " + description + " was found that matches \"" + stringToFind + '"');
  }

  @Test
  public void findMatchInResultSetGivenNullResultSetThrows() throws SQLException {
    var stringToFind = "stringToFind";
    ResultSet resultSet = null;
    var column = "column";
    var description = "description";

    assertThatThrownBy(
        () -> tableMetaDataManager.findMatchInResultSet(stringToFind, resultSet, column,
            description))
                .isInstanceOf(JdbcConnectorException.class)
                .hasMessageContaining(
                    "No " + description + " was found that matches \"" + stringToFind + '"');
  }

  @Test
  public void findMatchInResultSetGivenResultSetWithNoMatchThrows() throws SQLException {
    var stringToFind = "stringToFind";
    var column = "column";
    var description = "description";
    var resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    when(resultSet.getString(column)).thenReturn("doesNotMatch");

    assertThatThrownBy(
        () -> tableMetaDataManager.findMatchInResultSet(stringToFind, resultSet, column,
            description))
                .isInstanceOf(JdbcConnectorException.class)
                .hasMessageContaining(
                    "No " + description + " was found that matches \"" + stringToFind + '"');
  }

  @Test
  public void findMatchInResultSetGivenResultSetWithMultipleExactMatchesThrows()
      throws SQLException {
    var stringToFind = "stringToFind";
    var column = "column";
    var description = "description";
    var resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(resultSet.getString(column)).thenReturn("stringToFind");

    assertThatThrownBy(
        () -> tableMetaDataManager.findMatchInResultSet(stringToFind, resultSet, column,
            description))
                .isInstanceOf(JdbcConnectorException.class)
                .hasMessageContaining(
                    "Multiple " + description + "s were found that match \"" + stringToFind + '"');
  }

  @Test
  public void findMatchInResultSetGivenResultSetWithMultipleInexactMatchesThrows()
      throws SQLException {
    var stringToFind = "stringToFind";
    var column = "column";
    var description = "description";
    var resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(resultSet.getString(column)).thenReturn("STRINGToFind");

    assertThatThrownBy(
        () -> tableMetaDataManager.findMatchInResultSet(stringToFind, resultSet, column,
            description))
                .isInstanceOf(JdbcConnectorException.class)
                .hasMessageContaining(
                    "Multiple " + description + "s were found that match \"" + stringToFind + '"');
  }

  @Test
  public void findMatchInResultSetGivenResultSetWithOneInexactMatchReturnsIt() throws SQLException {
    var stringToFind = "stringToFind";
    var column = "column";
    var description = "description";
    var resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    var inexactMatch = "STRINGToFind";
    when(resultSet.getString(column)).thenReturn(inexactMatch);

    var result =
        tableMetaDataManager.findMatchInResultSet(stringToFind, resultSet, column, description);

    assertThat(result).isEqualTo(inexactMatch);
  }

  @Test
  public void findMatchInResultSetGivenResultSetWithOneExactMatchAndMultipleInexactReturnsTheExactMatch()
      throws SQLException {
    var stringToFind = "stringToFind";
    var column = "column";
    var description = "description";
    var resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
    var inexactMatch = "STRINGToFind";
    when(resultSet.getString(column)).thenReturn(inexactMatch).thenReturn(stringToFind)
        .thenReturn(inexactMatch);

    var result =
        tableMetaDataManager.findMatchInResultSet(stringToFind, resultSet, column, description);

    assertThat(result).isEqualTo(stringToFind);
  }

}
