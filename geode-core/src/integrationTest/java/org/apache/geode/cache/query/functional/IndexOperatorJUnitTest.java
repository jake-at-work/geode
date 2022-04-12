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
/*
 * CompileIndexOperatorTest.java
 *
 * Created on March 23, 2005, 4:52 PM
 */
package org.apache.geode.cache.query.functional;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.test.junit.categories.OQLIndexTest;

@Category({OQLIndexTest.class})
public class IndexOperatorJUnitTest {

  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testWithString() throws Exception {
    var str = "xyz";
    var c = (Character) runQuery(str, 0);
    if (c != 'x') {
      fail();
    }
    var d = (Character) runQuery(str, 2);
    if (d != 'z') {
      fail();
    }
  }

  @Test
  public void testWithArray() throws Exception {
    Object result = null;
    var index = 1;
    var stringArray = new String[] {"a", "b"};
    result = runQuery(stringArray, index);
    if (result == null || !stringArray[index].equals(result)) {
      fail("failed for String array");
    }

    var intArray = new int[] {1, 2};
    result = runQuery(intArray, index);
    if (result == null || intArray[index] != (Integer) result) {
      fail("failed for int array");
    }

    Object[] objectArray = {"a", "b"};
    result = runQuery(objectArray, index);
    if (result == null || !objectArray[index].equals(result)) {
      fail("failed for String array");
    }

  }

  @Test
  public void testWithList() throws Exception {
    var list = new ArrayList();
    list.add("aa");
    list.add("bb");
    Object result = null;
    var index = 1;
    result = runQuery(list, index);
    if (result == null || !list.get(index).equals(result)) {
      fail("failed for List");
    }
  }

  @Test
  public void testWithMap() throws Exception {

    var map = new HashMap();
    map.put("0", 11);
    map.put("1", 12);
    Object result = null;
    Object index = "1";
    result = runQuery(map, index);
    if (result == null || !map.get(index).equals(result)) {
      fail("failed for Map");
    }
  }

  @Test
  public void testWithRegion() throws Exception {

    var region = CacheUtils.createRegion("Portfolio", Portfolio.class);
    for (var i = 0; i < 5; i++) {
      region.put("" + i, new Portfolio(i));
    }
    Object result = null;
    Object index = "2";
    result = runQuery(region, index);
    if (result == null || !region.get(index).equals(result)) {
      fail("failed for Region");
    }
  }

  @Test
  public void testIndexOfIndex() throws Exception {
    var array = new String[] {"abc", "def"};
    var q = CacheUtils.getQueryService().newQuery("$1[0][0]");
    Object[] params = {array, 0};
    var result = (Character) q.execute(params);
    if (result == null || result != 'a') {
      fail();
    }
  }

  @Test
  public void testWithNULL() throws Exception {
    runQuery(null, 0);
    runQuery(null, null);
    Object[] objectArray = {"a", "b"};
    try {
      runQuery(objectArray, null);
      fail();
    } catch (TypeMismatchException ignored) {
    }
    var map = new HashMap();
    map.put("0", 11);
    map.put("1", 12);
    var result = runQuery(map, null);
    if (result != null) {
      fail();
    }
  }

  @Test
  public void testWithUNDEFINED() throws Exception {
    try {
      runQuery(QueryService.UNDEFINED, 0);
    } catch (TypeMismatchException e) {
      fail();
    }
    try {
      runQuery(QueryService.UNDEFINED, QueryService.UNDEFINED);
    } catch (TypeMismatchException e) {
      fail();
    }
    Object[] objectArray = {"a", "b"};
    try {
      runQuery(objectArray, QueryService.UNDEFINED);
      fail();
    } catch (TypeMismatchException ignored) {
    }
    var map = new HashMap();
    map.put("0", 11);
    map.put("1", 12);
    var result = runQuery(map, QueryService.UNDEFINED);
    if (result != null) {
      fail();
    }
  }

  @Test
  public void testWithUnsupportedArgs() throws Exception {
    try {
      runQuery("a", "a");
      fail();
    } catch (TypeMismatchException ignored) {
    }

    try {
      runQuery(new Object(), 0);
      fail();
    } catch (TypeMismatchException ignored) {
    }

    try {
      Object[] objectArray = {"a", "b"};
      runQuery(objectArray, new Object());
      fail();
    } catch (TypeMismatchException ignored) {
    }
  }

  public Object runQuery(Object array, Object index) throws Exception {
    var q = CacheUtils.getQueryService().newQuery("$1[$2]");
    var params = new Object[] {array, index};
    var result = q.execute(params);
    return result;
  }

  public Object runQuery(Object array, int index) throws Exception {
    var q = CacheUtils.getQueryService().newQuery("$1[$2]");
    var params = new Object[] {array, index};
    var result = q.execute(params);
    return result;
  }


}
