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
package org.apache.geode.management.internal.json;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.map;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import org.apache.geode.cache.query.data.CollectionHolder;
import org.apache.geode.internal.logging.DateFormatter;
import org.apache.geode.management.model.Employee;
import org.apache.geode.management.model.Item;
import org.apache.geode.management.model.Order;
import org.apache.geode.management.model.SubOrder;

public class QueryResultFormatterTest {
  private static final String RESULT = "result";

  private void checkResult(final QueryResultFormatter queryResultFormatter,
      String expectedJsonString) throws Exception {
    var jsonString = queryResultFormatter.toString();
    assertThat(jsonString).isEqualTo(expectedJsonString);

    var jsonObject = new ObjectMapper().readTree(jsonString);
    assertThat(jsonObject.get(RESULT)).isNotNull();
  }

  @Test
  public void canBeMocked() {
    var mockQueryResultFormatter = mock(QueryResultFormatter.class);
    var value = new Object();

    mockQueryResultFormatter.add("key", value);
    verify(mockQueryResultFormatter, times(1)).add("key", value);
  }

  @Test
  public void testPrimitives() throws Exception {
    var expectedByteString = "{\"result\":[[\"java.lang.Byte\",1]]}";
    var byteResult = new QueryResultFormatter(100).add(RESULT, (byte) 1);
    var boxedByteResult = new QueryResultFormatter(100).add(RESULT, new Byte("1"));
    checkResult(byteResult, expectedByteString);
    checkResult(boxedByteResult, expectedByteString);

    var expectedShortString = "{\"result\":[[\"java.lang.Short\",1]]}";
    var shortResult = new QueryResultFormatter(100).add(RESULT, (short) 1);
    var boxedShortResult =
        new QueryResultFormatter(100).add(RESULT, new Short("1"));
    checkResult(shortResult, expectedShortString);
    checkResult(boxedShortResult, expectedShortString);

    var expectedIntegerString = "{\"result\":[[\"java.lang.Integer\",1]]}";
    var integerResult = new QueryResultFormatter(100).add(RESULT, 1);
    var boxedIntegerResult =
        new QueryResultFormatter(100).add(RESULT, new Integer("1"));
    checkResult(integerResult, expectedIntegerString);
    checkResult(boxedIntegerResult, expectedIntegerString);

    var expectedLongString = "{\"result\":[[\"java.lang.Long\",25]]}";
    var longResult = new QueryResultFormatter(100).add(RESULT, 25L);
    var boxedLongResult =
        new QueryResultFormatter(100).add(RESULT, new Long("25"));
    checkResult(longResult, expectedLongString);
    checkResult(boxedLongResult, expectedLongString);

    var expectedFloatString = "{\"result\":[[\"java.lang.Float\",26.0]]}";
    var floatResult = new QueryResultFormatter(100).add(RESULT, 26f);
    var boxedFloatResult =
        new QueryResultFormatter(100).add(RESULT, new Float("26.0"));
    checkResult(floatResult, expectedFloatString);
    checkResult(boxedFloatResult, expectedFloatString);

    var expectedDoubleString = "{\"result\":[[\"java.lang.Double\",30.0]]}";
    var doubleResult = new QueryResultFormatter(100).add(RESULT, 30d);
    var boxedDoubleResult =
        new QueryResultFormatter(100).add(RESULT, new Double("30.0"));
    checkResult(doubleResult, expectedDoubleString);
    checkResult(boxedDoubleResult, expectedDoubleString);

    var expectedBooleanString = "{\"result\":[[\"java.lang.Boolean\",true]]}";
    var booleanResult = new QueryResultFormatter(100).add(RESULT, true);
    var boxedBooleanResult =
        new QueryResultFormatter(100).add(RESULT, Boolean.TRUE);
    checkResult(booleanResult, expectedBooleanString);
    checkResult(boxedBooleanResult, expectedBooleanString);

    var expectedCharString = "{\"result\":[[\"java.lang.Character\",\"a\"]]}";
    var charResult = new QueryResultFormatter(100).add(RESULT, 'a');
    var boxedCharResult =
        new QueryResultFormatter(100).add(RESULT, 'a');
    checkResult(charResult, expectedCharString);
    checkResult(boxedCharResult, expectedCharString);

    var stringResult = new QueryResultFormatter(100).add(RESULT, "String");
    checkResult(stringResult, "{\"result\":[[\"java.lang.String\",\"String\"]]}");
  }

  @Test
  public void testDateTimes() throws Exception {
    var time = System.currentTimeMillis();
    var date = new Date(time);
    var format = DateFormatter.createLocalizedDateFormat();
    var expectedString = format.format(date);
    var javaDateResult =
        new QueryResultFormatter(100).add(RESULT, date);
    checkResult(javaDateResult,
        "{\"result\":[[\"java.util.Date\",\"" + expectedString + "\"]]}");

    var sqlDate = new java.sql.Date(time);
    var sqlDateResult =
        new QueryResultFormatter(100).add(RESULT, sqlDate);
    checkResult(sqlDateResult,
        "{\"result\":[[\"java.sql.Date\",\"" + expectedString + "\"]]}");
  }

  @Test
  public void testCustomBeans() throws Exception {
    var object = new Object();
    var queryResultFormatter = new QueryResultFormatter(100).add(RESULT, object);
    checkResult(queryResultFormatter, "{\"result\":[[\"java.lang.Object\",{}]]}");

    var order = new Order();
    order.setId("order1");
    Collection<Item> items = new ArrayList<>();
    items.add(new Item(order, "item1", "itemDescription1"));
    order.setItems(items);
    var orderResult = new QueryResultFormatter(100).add(RESULT, order);
    checkResult(orderResult,
        "{\"result\":[[\"org.apache.geode.management.model.Order\",{\"id\":[\"java.lang.String\",\"order1\"],\"items\":[\"java.util.ArrayList\",{\"0\":[\"org.apache.geode.management.model.Item\",{\"itemDescription\":[\"java.lang.String\",\"itemDescription1\"],\"itemId\":[\"java.lang.String\",\"item1\"],\"order\":\"duplicate org.apache.geode.management.model.Order\"}]}]}]]}");
  }

  @Test
  public void testBigDecimal() throws Exception {
    var dc = new BigDecimal(20);

    var queryResultFormatter = new QueryResultFormatter(100).add(RESULT, dc);
    checkResult(queryResultFormatter, "{\"result\":[[\"java.math.BigDecimal\",20]]}");
  }

  @Test
  public void testArrayWithPrimitives() throws Exception {
    var byteArray = new Byte[] {0, 1, 2};
    var byteArrayResult = new QueryResultFormatter(100).add(RESULT, byteArray);
    checkResult(byteArrayResult, "{\"result\":[[\"java.lang.Byte[]\",[0,1,2]]]}");

    var shortArray = new Short[] {0, 1, 2};
    var shortArrayResult = new QueryResultFormatter(100).add(RESULT, shortArray);
    checkResult(shortArrayResult, "{\"result\":[[\"java.lang.Short[]\",[0,1,2]]]}");

    var integerArray = new Integer[] {0, 1, 2};
    var integerArrayResult =
        new QueryResultFormatter(100).add(RESULT, integerArray);
    checkResult(integerArrayResult, "{\"result\":[[\"java.lang.Integer[]\",[0,1,2]]]}");

    var longArray = new Long[] {0L, 1L, 2L};
    var longArrayResult = new QueryResultFormatter(100).add(RESULT, longArray);
    checkResult(longArrayResult, "{\"result\":[[\"java.lang.Long[]\",[0,1,2]]]}");

    var floatArray = new Float[] {0f, 1f, 2f};
    var floatArrayResult = new QueryResultFormatter(100).add(RESULT, floatArray);
    checkResult(floatArrayResult, "{\"result\":[[\"java.lang.Float[]\",[0.0,1.0,2.0]]]}");

    var doubleArray = new Double[] {0d, 1d, 2d};
    var doubleArrayResult = new QueryResultFormatter(100).add(RESULT, doubleArray);
    checkResult(doubleArrayResult, "{\"result\":[[\"java.lang.Double[]\",[0.0,1.0,2.0]]]}");

    var booleanArray = new Boolean[] {true, false};
    var booleanArrayResult =
        new QueryResultFormatter(100).add(RESULT, booleanArray);
    checkResult(booleanArrayResult, "{\"result\":[[\"java.lang.Boolean[]\",[true,false]]]}");

    var charArray = new Character[] {'a', 'b'};
    var charArrayResult = new QueryResultFormatter(100).add(RESULT, charArray);
    checkResult(charArrayResult, "{\"result\":[[\"java.lang.Character[]\",[\"a\",\"b\"]]]}");

    var stringArray = new String[] {"string_1", "string_2"};
    var stringArrayResult = new QueryResultFormatter(100).add(RESULT, stringArray);
    checkResult(stringArrayResult,
        "{\"result\":[[\"java.lang.String[]\",[\"string_1\",\"string_2\"]]]}");
  }

  @Test
  public void testArrayWithCustomBeans() throws Exception {
    var subOrderArray = new SubOrder[2];
    subOrderArray[0] = new SubOrder();
    var subOrderArrayResult =
        new QueryResultFormatter(100).add(RESULT, subOrderArray);
    checkResult(subOrderArrayResult,
        "{\"result\":[[\"org.apache.geode.management.model.SubOrder[]\",[{\"id\":[\"java.lang.String\",\"null1\"],\"items\":[\"java.util.ArrayList\",{}]},null]]]}");

    var arrayHolder = new CollectionHolder();
    var arrayHolderResult = new QueryResultFormatter(100).add(RESULT, arrayHolder);
    checkResult(arrayHolderResult,
        "{\"result\":[[\"org.apache.geode.cache.query.data.CollectionHolder\",{\"arr\":[\"java.lang.String[]\",[\"0\",\"1\",\"2\",\"3\",\"4\",\"SUN\",\"IBM\",\"YHOO\",\"GOOG\",\"MSFT\"]]}]]}");
  }

  @Test
  public void testArrayList() throws Exception {
    List<String> list = new ArrayList<>();
    list.add("ONE");
    list.add("TWO");
    list.add("THREE");

    var queryResultFormatter = new QueryResultFormatter(100).add(RESULT, list);
    checkResult(queryResultFormatter,
        "{\"result\":[[\"java.util.ArrayList\",{\"0\":[\"java.lang.String\",\"ONE\"],\"1\":[\"java.lang.String\",\"TWO\"],\"2\":[\"java.lang.String\",\"THREE\"]}]]}");
  }

  @Test
  public void testMap() throws Exception {
    Map<String, String> map = new HashMap<>();
    map.put("1", "ONE");
    map.put("2", "TWO");
    map.put("3", "THREE");
    map.put("4", "FOUR");

    var queryResultFormatter = new QueryResultFormatter(100).add(RESULT, map);
    checkResult(queryResultFormatter,
        "{\"result\":[[\"java.util.HashMap\",{\"1\":[\"java.lang.String\",\"ONE\"],\"2\":[\"java.lang.String\",\"TWO\"],\"3\":[\"java.lang.String\",\"THREE\"],\"4\":[\"java.lang.String\",\"FOUR\"]}]]}");
  }

  @Test
  public void testBigList() throws Exception {
    List<String> list = new ArrayList<>();
    for (var i = 0; i < 1000; i++) {
      list.add("BIG_COLL_" + i);
    }

    var queryResultFormatter = new QueryResultFormatter(100).add(RESULT, list);
    checkResult(queryResultFormatter,
        "{\"result\":[[\"java.util.ArrayList\",{\"0\":[\"java.lang.String\",\"BIG_COLL_0\"],\"1\":[\"java.lang.String\",\"BIG_COLL_1\"],\"2\":[\"java.lang.String\",\"BIG_COLL_2\"],\"3\":[\"java.lang.String\",\"BIG_COLL_3\"],\"4\":[\"java.lang.String\",\"BIG_COLL_4\"],\"5\":[\"java.lang.String\",\"BIG_COLL_5\"],\"6\":[\"java.lang.String\",\"BIG_COLL_6\"],\"7\":[\"java.lang.String\",\"BIG_COLL_7\"],\"8\":[\"java.lang.String\",\"BIG_COLL_8\"],\"9\":[\"java.lang.String\",\"BIG_COLL_9\"],\"10\":[\"java.lang.String\",\"BIG_COLL_10\"],\"11\":[\"java.lang.String\",\"BIG_COLL_11\"],\"12\":[\"java.lang.String\",\"BIG_COLL_12\"],\"13\":[\"java.lang.String\",\"BIG_COLL_13\"],\"14\":[\"java.lang.String\",\"BIG_COLL_14\"],\"15\":[\"java.lang.String\",\"BIG_COLL_15\"],\"16\":[\"java.lang.String\",\"BIG_COLL_16\"],\"17\":[\"java.lang.String\",\"BIG_COLL_17\"],\"18\":[\"java.lang.String\",\"BIG_COLL_18\"],\"19\":[\"java.lang.String\",\"BIG_COLL_19\"],\"20\":[\"java.lang.String\",\"BIG_COLL_20\"],\"21\":[\"java.lang.String\",\"BIG_COLL_21\"],\"22\":[\"java.lang.String\",\"BIG_COLL_22\"],\"23\":[\"java.lang.String\",\"BIG_COLL_23\"],\"24\":[\"java.lang.String\",\"BIG_COLL_24\"],\"25\":[\"java.lang.String\",\"BIG_COLL_25\"],\"26\":[\"java.lang.String\",\"BIG_COLL_26\"],\"27\":[\"java.lang.String\",\"BIG_COLL_27\"],\"28\":[\"java.lang.String\",\"BIG_COLL_28\"],\"29\":[\"java.lang.String\",\"BIG_COLL_29\"],\"30\":[\"java.lang.String\",\"BIG_COLL_30\"],\"31\":[\"java.lang.String\",\"BIG_COLL_31\"],\"32\":[\"java.lang.String\",\"BIG_COLL_32\"],\"33\":[\"java.lang.String\",\"BIG_COLL_33\"],\"34\":[\"java.lang.String\",\"BIG_COLL_34\"],\"35\":[\"java.lang.String\",\"BIG_COLL_35\"],\"36\":[\"java.lang.String\",\"BIG_COLL_36\"],\"37\":[\"java.lang.String\",\"BIG_COLL_37\"],\"38\":[\"java.lang.String\",\"BIG_COLL_38\"],\"39\":[\"java.lang.String\",\"BIG_COLL_39\"],\"40\":[\"java.lang.String\",\"BIG_COLL_40\"],\"41\":[\"java.lang.String\",\"BIG_COLL_41\"],\"42\":[\"java.lang.String\",\"BIG_COLL_42\"],\"43\":[\"java.lang.String\",\"BIG_COLL_43\"],\"44\":[\"java.lang.String\",\"BIG_COLL_44\"],\"45\":[\"java.lang.String\",\"BIG_COLL_45\"],\"46\":[\"java.lang.String\",\"BIG_COLL_46\"],\"47\":[\"java.lang.String\",\"BIG_COLL_47\"],\"48\":[\"java.lang.String\",\"BIG_COLL_48\"],\"49\":[\"java.lang.String\",\"BIG_COLL_49\"],\"50\":[\"java.lang.String\",\"BIG_COLL_50\"],\"51\":[\"java.lang.String\",\"BIG_COLL_51\"],\"52\":[\"java.lang.String\",\"BIG_COLL_52\"],\"53\":[\"java.lang.String\",\"BIG_COLL_53\"],\"54\":[\"java.lang.String\",\"BIG_COLL_54\"],\"55\":[\"java.lang.String\",\"BIG_COLL_55\"],\"56\":[\"java.lang.String\",\"BIG_COLL_56\"],\"57\":[\"java.lang.String\",\"BIG_COLL_57\"],\"58\":[\"java.lang.String\",\"BIG_COLL_58\"],\"59\":[\"java.lang.String\",\"BIG_COLL_59\"],\"60\":[\"java.lang.String\",\"BIG_COLL_60\"],\"61\":[\"java.lang.String\",\"BIG_COLL_61\"],\"62\":[\"java.lang.String\",\"BIG_COLL_62\"],\"63\":[\"java.lang.String\",\"BIG_COLL_63\"],\"64\":[\"java.lang.String\",\"BIG_COLL_64\"],\"65\":[\"java.lang.String\",\"BIG_COLL_65\"],\"66\":[\"java.lang.String\",\"BIG_COLL_66\"],\"67\":[\"java.lang.String\",\"BIG_COLL_67\"],\"68\":[\"java.lang.String\",\"BIG_COLL_68\"],\"69\":[\"java.lang.String\",\"BIG_COLL_69\"],\"70\":[\"java.lang.String\",\"BIG_COLL_70\"],\"71\":[\"java.lang.String\",\"BIG_COLL_71\"],\"72\":[\"java.lang.String\",\"BIG_COLL_72\"],\"73\":[\"java.lang.String\",\"BIG_COLL_73\"],\"74\":[\"java.lang.String\",\"BIG_COLL_74\"],\"75\":[\"java.lang.String\",\"BIG_COLL_75\"],\"76\":[\"java.lang.String\",\"BIG_COLL_76\"],\"77\":[\"java.lang.String\",\"BIG_COLL_77\"],\"78\":[\"java.lang.String\",\"BIG_COLL_78\"],\"79\":[\"java.lang.String\",\"BIG_COLL_79\"],\"80\":[\"java.lang.String\",\"BIG_COLL_80\"],\"81\":[\"java.lang.String\",\"BIG_COLL_81\"],\"82\":[\"java.lang.String\",\"BIG_COLL_82\"],\"83\":[\"java.lang.String\",\"BIG_COLL_83\"],\"84\":[\"java.lang.String\",\"BIG_COLL_84\"],\"85\":[\"java.lang.String\",\"BIG_COLL_85\"],\"86\":[\"java.lang.String\",\"BIG_COLL_86\"],\"87\":[\"java.lang.String\",\"BIG_COLL_87\"],\"88\":[\"java.lang.String\",\"BIG_COLL_88\"],\"89\":[\"java.lang.String\",\"BIG_COLL_89\"],\"90\":[\"java.lang.String\",\"BIG_COLL_90\"],\"91\":[\"java.lang.String\",\"BIG_COLL_91\"],\"92\":[\"java.lang.String\",\"BIG_COLL_92\"],\"93\":[\"java.lang.String\",\"BIG_COLL_93\"],\"94\":[\"java.lang.String\",\"BIG_COLL_94\"],\"95\":[\"java.lang.String\",\"BIG_COLL_95\"],\"96\":[\"java.lang.String\",\"BIG_COLL_96\"],\"97\":[\"java.lang.String\",\"BIG_COLL_97\"],\"98\":[\"java.lang.String\",\"BIG_COLL_98\"],\"99\":[\"java.lang.String\",\"BIG_COLL_99\"]}]]}");
  }

  @Test
  public void testEnum() throws Exception {
    var queryResultFormatter =
        new QueryResultFormatter(100).add(RESULT, Currency.DIME);
    checkResult(queryResultFormatter,
        "{\"result\":[[\"org.apache.geode.management.internal.json.QueryResultFormatterTest.Currency\",\"DIME\"]]}");
  }

  @Test
  public void testEnumList() throws Exception {
    List<Currency> list = new ArrayList<>();
    list.add(Currency.DIME);
    list.add(Currency.NICKLE);
    list.add(Currency.QUARTER);
    list.add(Currency.NICKLE);

    var queryResultFormatter = new QueryResultFormatter(100).add(RESULT, list);
    checkResult(queryResultFormatter,
        "{\"result\":[[\"java.util.ArrayList\",{\"0\":[\"org.apache.geode.management.internal.json.QueryResultFormatterTest.Currency\",\"DIME\"],\"1\":[\"org.apache.geode.management.internal.json.QueryResultFormatterTest.Currency\",\"NICKLE\"],\"2\":[\"org.apache.geode.management.internal.json.QueryResultFormatterTest.Currency\",\"QUARTER\"],\"3\":[\"org.apache.geode.management.internal.json.QueryResultFormatterTest.Currency\",\"NICKLE\"]}]]}");
  }

  @Test
  public void testEnumContainer() throws Exception {
    var enumContainer = new EnumContainer(Currency.DIME);
    var queryResultFormatter =
        new QueryResultFormatter(100).add(RESULT, enumContainer);
    checkResult(queryResultFormatter,
        "{\"result\":[[\"org.apache.geode.management.internal.json.QueryResultFormatterTest.EnumContainer\",{}]]}");
  }

  @Test
  public void testObjectWithJsonAnnotation() throws Exception {
    var employee = new Employee();
    employee.setId(10);
    employee.setName("John");
    employee.setTitle("Manager");
    var queryResultFormatter = new QueryResultFormatter(100);
    queryResultFormatter.add("result", employee);
    // these to make sure we are keeping the pre 1.8 behavior
    assertThat(queryResultFormatter.toString())
        // make sure null values are serialized as well
        .contains("\"address\":null")
        // make sure we don't honor @JsonIgnore annotation
        .contains("id")
        // make sure we don't honor @JsonProperty annotation
        .doesNotContain("Job Title");
  }

  private enum Currency {
    PENNY, NICKLE, DIME, QUARTER
  }

  private static class EnumContainer {
    private final Currency currency;

    EnumContainer(final Currency currency) {
      this.currency = currency;
    }
  }
}
