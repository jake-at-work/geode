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
package org.apache.geode.security.query;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.security.query.data.QueryTestObject;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@RunWith(Parameterized.class)
@Category({SecurityTest.class, OQLQueryTest.class})
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class QuerySecurityAllowedQueriesDistributedTest
    extends AbstractQuerySecurityDistributedTest {

  @Parameterized.Parameters(name = "User:{0}, RegionType:{1}")
  public static Object[] usersAndRegionTypes() {
    return new Object[][] {
        {"super-user", REPLICATE}, {"super-user", PARTITION},
        {"dataReader", REPLICATE}, {"dataReader", PARTITION},
        {"dataReaderRegion", REPLICATE}, {"dataReaderRegion", PARTITION},
        {"clusterManagerDataReader", REPLICATE}, {"clusterManagerDataReader", PARTITION},
        {"clusterManagerDataReaderRegion", REPLICATE}, {"clusterManagerDataReaderRegion", PARTITION}
    };
  }

  @Parameterized.Parameter
  public String user;

  @Parameterized.Parameter(1)
  public RegionShortcut regionShortcut;

  @Before
  public void setUp() throws Exception {
    super.setUp(user, regionShortcut);

    keys = new Object[] {"key-0", "key-1"};
    values = new Object[] {
        new QueryTestObject(1, "John"),
        new QueryTestObject(3, "Beth")
    };

    putIntoRegion(superUserClient, keys, values, regionName);
  }

  @Test
  public void queryWithSelectStarShouldNotThrowSecurityException() {
    var query = "SELECT * FROM " + SEPARATOR + regionName;
    var expectedResults = Arrays.asList(values);
    executeQueryAndAssertExpectedResults(specificUserClient, query, expectedResults);
  }

  /* ----- Public Field Tests ----- */
  @Test
  public void queryWithPublicFieldOnWhereClauseShouldNotThrowSecurityException() {
    var query = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.id = 1";
    var expectedResults = Collections.singletonList(values[0]);
    executeQueryAndAssertExpectedResults(specificUserClient, query, expectedResults);
  }

  @Test
  public void queryingByPublicFieldOnSelectClauseShouldNotThrowSecurityException() {
    var query = "SELECT r.id FROM " + SEPARATOR + regionName + " r";
    List<Object> expectedResults = Arrays.asList(1, 3);
    executeQueryAndAssertExpectedResults(specificUserClient, query, expectedResults);
  }

  @Test
  public void queriesWithPublicFieldUsedWithinAggregateFunctionsShouldNotThrowSecurityException() {
    var queryCount = "SELECT COUNT(r.id) FROM " + SEPARATOR + regionName + " r";
    executeQueryAndAssertExpectedResults(specificUserClient, queryCount,
        Collections.singletonList(2));

    var queryMax = "SELECT MAX(r.id) FROM " + SEPARATOR + regionName + " r";
    executeQueryAndAssertExpectedResults(specificUserClient, queryMax,
        Collections.singletonList(3));

    var queryMin = "SELECT MIN(r.id) FROM " + SEPARATOR + regionName + " r";
    executeQueryAndAssertExpectedResults(specificUserClient, queryMin,
        Collections.singletonList(1));

    var queryAvg = "SELECT AVG(r.id) FROM " + SEPARATOR + regionName + " r";
    executeQueryAndAssertExpectedResults(specificUserClient, queryAvg,
        Collections.singletonList(2));

    var querySum = "SELECT SUM(r.id) FROM " + SEPARATOR + regionName + " r";
    executeQueryAndAssertExpectedResults(specificUserClient, querySum,
        Collections.singletonList(4));
  }

  @Test
  public void queryWithPublicFieldUsedWithinDistinctClauseShouldNotThrowSecurityException() {
    var query = "<TRACE> SELECT DISTINCT * from " + SEPARATOR + regionName
        + " WHERE id IN SET(1, 3) ORDER BY id asc LIMIT 2";
    executeQueryAndAssertExpectedResults(specificUserClient, query, Arrays.asList(values));
  }

  @Test
  public void queryWithByPublicFieldOnInnerQueriesShouldNotThrowSecurityException() {
    var query = "SELECT * FROM " + SEPARATOR + regionName
        + " r1 WHERE r1.id IN (SELECT r2.id FROM " + SEPARATOR
        + regionName + " r2)";
    var expectedResults = Arrays.asList(values);
    executeQueryAndAssertExpectedResults(specificUserClient, query, expectedResults);
  }

  /* ----- Default Allowed Methods Tests ----- */
  @Test
  public void queriesWithAllowedRegionMethodInvocationsShouldNotThrowSecurityException() {
    var queryValues = "SELECT * FROM " + SEPARATOR + regionName + ".values";
    executeQueryAndAssertExpectedResults(specificUserClient, queryValues, Arrays.asList(values));

    var queryKeySet = "SELECT * FROM " + SEPARATOR + regionName + ".keySet";
    executeQueryAndAssertExpectedResults(specificUserClient, queryKeySet, Arrays.asList(keys));

    var queryContainsKey =
        "SELECT * FROM " + SEPARATOR + regionName + ".containsKey('" + keys[0] + "')";
    executeQueryAndAssertExpectedResults(specificUserClient, queryContainsKey,
        Collections.singletonList(true));

    var queryEntrySet = "SELECT * FROM " + SEPARATOR + regionName + ".get('" + keys[0] + "')";
    executeQueryAndAssertExpectedResults(specificUserClient, queryEntrySet,
        Collections.singletonList(values[0]));
  }

  @Test
  public void queriesWithAllowedRegionEntryMethodInvocationsShouldNotThrowSecurityException() {
    var expectedKeys = Arrays.asList(keys);
    var queryKeyEntrySet = "SELECT e.key FROM " + SEPARATOR + regionName + ".entrySet e";
    executeQueryAndAssertExpectedResults(specificUserClient, queryKeyEntrySet, expectedKeys);
    var queryGetKeyEntrySet = "SELECT e.getKey FROM " + SEPARATOR + regionName + ".entrySet e";
    executeQueryAndAssertExpectedResults(specificUserClient, queryGetKeyEntrySet, expectedKeys);
    var queryKeyEntries = "SELECT e.key FROM " + SEPARATOR + regionName + ".entries e";
    executeQueryAndAssertExpectedResults(specificUserClient, queryKeyEntries, expectedKeys);
    var queryGetKeyEntries = "SELECT e.getKey FROM " + SEPARATOR + regionName + ".entries e";
    executeQueryAndAssertExpectedResults(specificUserClient, queryGetKeyEntries, expectedKeys);

    var expectedValues = Arrays.asList(values);
    var queryValueEntrySet = "SELECT e.value FROM " + SEPARATOR + regionName + ".entrySet e";
    executeQueryAndAssertExpectedResults(specificUserClient, queryValueEntrySet, expectedValues);
    var queryGetValueEntrySet =
        "SELECT e.getValue FROM " + SEPARATOR + regionName + ".entrySet e";
    executeQueryAndAssertExpectedResults(specificUserClient, queryGetValueEntrySet, expectedValues);
    var queryValueEntries = "SELECT e.value FROM " + SEPARATOR + regionName + ".entries e";
    executeQueryAndAssertExpectedResults(specificUserClient, queryValueEntries, expectedValues);
    var queryGetValueEntries = "SELECT e.getValue FROM " + SEPARATOR + regionName + ".entries e";
    executeQueryAndAssertExpectedResults(specificUserClient, queryGetValueEntries, expectedValues);
  }

  @Test
  public void queriesWithAllowedStringMethodInvocationsShouldNotThrowSecurityException() {
    var toStringWhere =
        "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.toString = 'Test_Object'";
    executeQueryAndAssertExpectedResults(specificUserClient, toStringWhere, Arrays.asList(values));

    var toStringSelect = "SELECT r.toString() FROM " + SEPARATOR + regionName + " r";
    executeQueryAndAssertExpectedResults(specificUserClient, toStringSelect,
        Arrays.asList("Test_Object", "Test_Object"));

    var toUpperCaseWhere =
        "SELECT * FROM " + SEPARATOR + regionName
            + " r WHERE r.toString().toUpperCase = 'TEST_OBJECT'";
    executeQueryAndAssertExpectedResults(specificUserClient, toUpperCaseWhere,
        Arrays.asList(values));

    var toLowerCaseWhere =
        "SELECT * FROM " + SEPARATOR + regionName
            + " r WHERE r.toString().toLowerCase = 'test_object'";
    executeQueryAndAssertExpectedResults(specificUserClient, toLowerCaseWhere,
        Arrays.asList(values));
  }

  @Test
  public void queriesWithAllowedNumberMethodInvocationsShouldNotThrowSecurityException() {
    var intValue = "SELECT r.id.intValue() FROM " + SEPARATOR + regionName + " r";
    executeQueryAndAssertExpectedResults(specificUserClient, intValue, Arrays.asList(1, 3));

    var longValue = "SELECT r.id.longValue() FROM " + SEPARATOR + regionName + " r";
    executeQueryAndAssertExpectedResults(specificUserClient, longValue, Arrays.asList(1L, 3L));

    var doubleValue = "SELECT r.id.doubleValue() FROM " + SEPARATOR + regionName + " r";
    executeQueryAndAssertExpectedResults(specificUserClient, doubleValue, Arrays.asList(1d, 3d));

    var shortValue = "SELECT r.id.shortValue() FROM " + SEPARATOR + regionName + " r";
    executeQueryAndAssertExpectedResults(specificUserClient, shortValue,
        Arrays.asList((short) 1, (short) 3));
  }

  @Test
  public void queriesWithAllowedDateMethodInvocationsShouldNotThrowSecurityException() {
    var query =
        "SELECT * FROM " + SEPARATOR + regionName
            + " WHERE dateField = to_date('08/08/2018', 'MM/dd/yyyy')";
    var obj1 = new QueryTestObject(0, "John");
    obj1.setDateField("08/08/2018");
    var obj2 = new QueryTestObject(3, "Beth");
    obj2.setDateField("08/08/2018");
    Object[] values = {obj1, obj2};
    putIntoRegion(superUserClient, keys, values, regionName);

    executeQueryAndAssertExpectedResults(specificUserClient, query, Arrays.asList(values));
  }

  @Test
  public void queriesWithAllowedMapMethodInvocationsShouldNotThrowSecurityException() {
    var valueObject1 = new QueryTestObject(1, "John");
    Map<Object, Object> map1 = new HashMap<>();
    map1.put("intData", 1);
    map1.put(1, 98);
    map1.put("strData1", "ABC");
    map1.put("strData2", "ZZZ");
    valueObject1.mapField = map1;

    var valueObject2 = new QueryTestObject(3, "Beth");
    Map<Object, Object> map2 = new HashMap<>();
    map2.put("intData", 99);
    map2.put(1, 99);
    map2.put("strData1", "XYZ");
    map2.put("strData2", "ZZZ");
    valueObject2.mapField = map2;

    values = new Object[] {valueObject1, valueObject2};
    putIntoRegion(superUserClient, keys, values, regionName);

    var query1 = String.format(
        "SELECT * FROM " + SEPARATOR
            + "%s WHERE mapField.get('intData') = 1 AND mapField.get(1) = 98 AND mapField.get('strData1') = 'ABC' AND mapField.get('strData2') = 'ZZZ'",
        regionName);
    executeQueryAndAssertExpectedResults(specificUserClient, query1,
        Arrays.asList(new Object[] {valueObject1}));

    var query2 =
        String.format("SELECT * FROM " + SEPARATOR + "%s WHERE mapField.get('strData2') = 'ZZZ'",
            regionName);
    executeQueryAndAssertExpectedResults(specificUserClient, query2, Arrays.asList(values));
  }
}
