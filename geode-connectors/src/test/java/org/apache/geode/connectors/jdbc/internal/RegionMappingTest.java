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

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;

public class RegionMappingTest {

  private String name;
  private String fieldName1;

  private RegionMapping mapping;

  @Before
  public void setUp() {
    name = "name";
    fieldName1 = "myField1";
  }

  @Test
  public void initiatedWithNullValues() {
    mapping = new RegionMapping(null, "pdxClassName", null, null, null, null, null);

    assertThat(mapping.getTableName()).isNull();
    assertThat(mapping.getRegionName()).isNull();
    assertThat(mapping.getDataSourceName()).isNull();
    assertThat(mapping.getPdxName()).isEqualTo("pdxClassName");
    assertThat(mapping.getIds()).isNull();
    assertThat(mapping.getCatalog()).isNull();
    assertThat(mapping.getSchema()).isNull();
  }

  @Test
  public void hasCorrectTableName() {
    mapping = new RegionMapping(null, null, name, null, null, null, null);

    assertThat(mapping.getTableName()).isEqualTo(name);
  }

  @Test
  public void hasCorrectRegionName() {
    mapping = new RegionMapping(name, null, null, null, null, null, null);

    assertThat(mapping.getRegionName()).isEqualTo(name);
  }

  @Test
  public void hasCorrectConfigName() {
    mapping = new RegionMapping(null, null, null, name, null, null, null);

    assertThat(mapping.getDataSourceName()).isEqualTo(name);
  }

  @Test
  public void hasCorrectPdxClassName() {
    mapping = new RegionMapping(null, name, null, null, null, null, null);

    assertThat(mapping.getPdxName()).isEqualTo(name);
  }

  @Test
  public void hasCorrectIds() {
    var ids = "ids";
    mapping = new RegionMapping(null, null, null, null, ids, null, null);

    assertThat(mapping.getIds()).isEqualTo(ids);
  }

  @Test
  public void hasCorrectCatalog() {
    var catalog = "catalog";
    mapping = new RegionMapping(null, null, null, null, null, catalog, null);

    assertThat(mapping.getCatalog()).isEqualTo(catalog);
  }

  @Test
  public void hasCorrectSchema() {
    var schema = "schema";
    mapping = new RegionMapping(null, null, null, null, null, null, schema);

    assertThat(mapping.getSchema()).isEqualTo(schema);
  }

  @Test
  public void verifyTwoDefaultInstancesAreEqual() {
    var rm1 =
        new RegionMapping("regionName", "pdxClassName", null, "dataSourceName", null, null, null);
    var rm2 =
        new RegionMapping("regionName", "pdxClassName", null, "dataSourceName", null, null, null);
    assertThat(rm1).isEqualTo(rm2);
  }

  @Test
  public void verifyTwoInstancesThatAreEqualHaveSameHashCode() {
    var rm1 = new RegionMapping("regionName",
        "pdxClassName", "tableName", "dataSourceName", "ids", "catalog", "schema");

    var rm2 = new RegionMapping("regionName",
        "pdxClassName", "tableName", "dataSourceName", "ids", "catalog", "schema");

    assertThat(rm1.hashCode()).isEqualTo(rm2.hashCode());
  }

  @Test
  public void verifyToStringGivenAllAttributes() {
    var rm = new RegionMapping("regionName", "pdxClassName", "tableName",
        "dataSourceName", "ids", "catalog", "schema");
    rm.addFieldMapping(new FieldMapping("pdxName", "pdxType", "jdbcName", "jdbcType", true));

    var result = rm.toString();

    assertThat(result).isEqualTo(
        "RegionMapping{regionName='regionName', pdxName='pdxClassName', tableName='tableName', dataSourceName='dataSourceName', ids='ids', specifiedIds='true', catalog='catalog', schema='schema', fieldMapping='[FieldMapping [pdxName=pdxName, pdxType=pdxType, jdbcName=jdbcName, jdbcType=jdbcType, jdbcNullable=true]]'}");
  }

  @Test
  public void verifyToStringGivenRequiredAttributes() {
    var rm =
        new RegionMapping("regionName", "pdxClassName", null, "dataSourceName", null, null, null);

    var result = rm.toString();

    assertThat(result).isEqualTo(
        "RegionMapping{regionName='regionName', pdxName='pdxClassName', tableName='null', dataSourceName='dataSourceName', ids='null', specifiedIds='false', catalog='null', schema='null', fieldMapping='[]'}");
  }

  @Test
  public void verifyThatMappingIsEqualToItself() {
    mapping = new RegionMapping(null, "pdxClassName", null, null, null, null, null);
    var result = mapping.equals(mapping);
    assertThat(mapping.hashCode()).isEqualTo(mapping.hashCode());
    assertThat(result).isTrue();
  }

  @Test
  public void verifyThatNullIsNotEqual() {
    mapping = new RegionMapping(null, null, null, null, null, null, null);
    var result = mapping.equals(null);
    assertThat(result).isFalse();
  }

  @Test
  public void verifyOtherClassIsNotEqual() {
    mapping = new RegionMapping(null, null, null, null, null, null, null);
    var result = mapping.equals("not equal");
    assertThat(result).isFalse();
  }

  @Test
  public void verifyMappingWithDifferentRegionNamesAreNotEqual() {
    var rm1 =
        new RegionMapping(null, null, null, null, null, null, null);
    var rm2 =
        new RegionMapping("name", null, null, null, null, null, null);
    var result = rm1.equals(rm2);
    assertThat(result).isFalse();
  }

  @Test
  public void verifyMappingWithDifferentPdxClassNameAreNotEqual() {
    var rm1 =
        new RegionMapping(null, "pdxClassName", null, null, null, null, null);
    var rm2 =
        new RegionMapping(null, "pdxClass", null, null, null, null, null);
    var result = rm1.equals(rm2);
    assertThat(result).isFalse();
  }

  @Test
  public void verifyMappingWithDifferentTablesAreNotEqual() {
    var rm1 =
        new RegionMapping(null, "pdxClassName", "table1", null, null, null, null);
    var rm2 =
        new RegionMapping(null, "pdxClassName", "table2", null, null, null, null);
    var result = rm1.equals(rm2);
    assertThat(result).isFalse();
  }

  @Test
  public void verifyMappingWithDifferentDataSourcesAreNotEqual() {
    var rm1 =
        new RegionMapping(null, "pdxClassName", null, "datasource1", null, null, null);
    var rm2 =
        new RegionMapping(null, "pdxClassName", null, "datasource2", null, null, null);
    var result = rm1.equals(rm2);
    assertThat(result).isFalse();
  }

  @Test
  public void verifyMappingWithDifferentIdsAreNotEqual() {
    var rm1 =
        new RegionMapping(null, "pdxClassName", null, null, "ids1", null, null);
    var rm2 =
        new RegionMapping(null, "pdxClassName", null, null, "ids2", null, null);
    var result = rm1.equals(rm2);
    assertThat(result).isFalse();
  }

  @Test
  public void verifyMappingWithDifferentCatalogsAreNotEqual() {
    var rm1 =
        new RegionMapping(null, "pdxClassName", null, null, null, "catalog1", null);
    var rm2 =
        new RegionMapping(null, "pdxClassName", null, null, null, "catalog2", null);
    var result = rm1.equals(rm2);
    assertThat(result).isFalse();
  }

  @Test
  public void verifyMappingWithDifferentSchemasAreNotEqual() {
    var rm1 =
        new RegionMapping(null, "pdxClassName", null, null, null, null, "schema1");
    var rm2 =
        new RegionMapping(null, "pdxClassName", null, null, null, null, "schema2");
    var result = rm1.equals(rm2);
    assertThat(result).isFalse();
  }

  @Test
  public void verifyMappingWithDifferentFieldMappingsAreNotEqual() {
    var rm1 =
        new RegionMapping(null, "pdxClassName", null, null, null, null, null);
    rm1.addFieldMapping(
        new FieldMapping("myPdxName", "myPdxType", "myJdbcName", "myJdbcType", false));
    var rm2 =
        new RegionMapping(null, "pdxClassName", null, null, null, null, null);
    rm2.addFieldMapping(
        new FieldMapping("myPdxName", "myPdxType", "myJdbcName", "myOtherJdbcType", false));
    var result = rm1.equals(rm2);
    assertThat(result).isFalse();
  }

}
