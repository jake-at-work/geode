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
 * IteratorTypeDefIntegerTest_vj.java
 *
 * Created on April 12, 2005, 3:52 PM
 */

package org.apache.geode.cache.query.functional;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Student;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class IteratorTypeDefaultTypesJUnitTest {

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();

  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testIteratorDefIntegerArray() throws Exception {
    var a = new Integer[2];
    for (var j = 0; j < 2; j++) {
      a[j] = j;
    }
    var params = new Object[1];
    params[0] = a;

    var queries = new String[] {"Select distinct intValue from $1 TYPE int",
        "Select distinct intValue from (array<int>) $1 "

    };
    for (final var query : queries) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(query);
        var rs = (SelectResults) q.execute(params);
        if (rs.size() < 1) {
          fail("testIteratorDefIntegerArray: Query fetched zero results ");
        }

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
  }

  @Test
  public void testIteratorDefIntegerArrayList() throws Exception {

    var Arlist = new ArrayList();
    Arlist.add(11);
    Arlist.add(12);

    var params = new Object[1];
    params[0] = Arlist;

    var queries = new String[] {"Select distinct intValue from $1 TYPE int",
        "Select distinct intValue from (list<int>) $1"};
    for (final var query : queries) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(query);
        var rs = (SelectResults) q.execute(params);
        if (rs.size() < 1) {
          fail("testIteratorDefIntegerArrayList: Query fetched zero results ");
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

  }

  @Test
  public void testIteratorDefString() throws Exception {
    var s1 = "AA";
    var s2 = "BB";
    var C1 = new HashSet();
    C1.add(s1);
    C1.add(s2);
    var params = new Object[1];
    params[0] = C1;
    var queries = new String[] {"SELECT DISTINCT intern from (set<string>) $1",
        "SELECT DISTINCT intern from $1 TYPE string"};
    for (final var query : queries) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(query);
        var rs = (SelectResults) q.execute(params);
        if (rs.size() < 1) {
          fail("testIteratorDefString: Query fetched zero results ");
        }

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
  }

  @Test
  public void testIteratorDefBoolean() throws Exception {
    var b1 = true;
    var b2 = false;
    var C1 = new HashSet();
    C1.add(b1);
    C1.add(b2);
    var params = new Object[1];
    params[0] = C1;
    var queries = new String[] {"SELECT DISTINCT booleanValue from (set<boolean>) $1",
        "SELECT DISTINCT booleanValue from $1 TYPE boolean"};
    for (final var query : queries) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(query);
        var rs = (SelectResults) q.execute(params);
        if (rs.size() < 1) {
          fail("testIteratorDefBoolean: Query fetched zero results ");
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

  }

  @Test
  public void testIteratorDefByte() throws Exception {
    byte b1 = 1;
    byte b2 = 2;
    var C1 = new HashSet();
    C1.add(b1);
    C1.add(b2);
    var params = new Object[1];
    params[0] = C1;
    var queries = new String[] {"SELECT DISTINCT byteValue from (set<byte>) $1",
        "SELECT DISTINCT byteValue from $1 TYPE byte"};
    for (final var query : queries) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(query);
        var rs = (SelectResults) q.execute(params);
        if (rs.size() < 1) {
          fail("testIteratorDefByte: Query fetched zero results ");
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
  }

  @Test
  public void testIteratorDefShort() throws Exception {
    short sh1 = 11;
    short sh2 = 22;
    var C1 = new HashSet();
    C1.add(sh1);
    C1.add(sh2);
    var params = new Object[1];
    params[0] = C1;
    var queries = new String[] {"SELECT DISTINCT shortValue from (set<short>) $1",
        "SELECT DISTINCT shortValue from $1 TYPE short"};
    for (final var query : queries) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(query);
        var rs = (SelectResults) q.execute(params);
        if (rs.size() < 1) {
          fail("testIteratorDefShort: Query fetched zero results ");
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
  }

  @Test
  public void testIteratorDefLong() throws Exception {
    long lg1 = 111;
    long lg2 = 222;
    var C1 = new HashSet();
    C1.add(lg1);
    C1.add(lg2);
    var params = new Object[1];
    params[0] = C1;
    var queries = new String[] {"SELECT DISTINCT longValue from (set<long>) $1",
        "SELECT DISTINCT longValue from $1 TYPE long"};
    for (final var query : queries) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(query);
        var rs = (SelectResults) q.execute(params);
        if (rs.size() < 1) {
          fail("testIteratorDefLong: Query fetched zero results ");
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
  }

  @Test
  public void testIteratorDefDouble() throws Exception {
    var d1 = 1.11;
    var d2 = 2.22;
    var C1 = new HashSet();
    C1.add(d1);
    C1.add(d2);
    var params = new Object[1];
    params[0] = C1;
    var queries = new String[] {"SELECT DISTINCT doubleValue from (set<double>) $1",
        "SELECT DISTINCT doubleValue from $1 TYPE double"};
    for (final var query : queries) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(query);
        var rs = (SelectResults) q.execute(params);
        if (rs.size() < 1) {
          fail("testIteratorDefDouble: Query fetched zero results ");
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
  }

  @Test
  public void testIteratorDefFloat() throws Exception {
    float fl1 = 1;
    float fl2 = 2;
    var C1 = new HashSet();
    C1.add(fl1);
    C1.add(fl2);
    var params = new Object[1];
    params[0] = C1;
    var queries = new String[] {"SELECT DISTINCT floatValue from (set<float>) $1",
        "SELECT DISTINCT floatValue from $1 TYPE float"};
    for (final var query : queries) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(query);
        var rs = (SelectResults) q.execute(params);
        if (rs.size() < 1) {
          fail("testIteratorDefFloat: Query fetched zero results ");
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
  }

  @Test
  public void testIteratorDefChar() throws Exception {
    var ch1 = 'a';
    var ch2 = 'z';
    var C1 = new HashSet();
    C1.add(ch1);
    C1.add(ch2);
    var params = new Object[1];
    params[0] = C1;
    var queries = new String[] {"SELECT DISTINCT charValue from (set<char>) $1",
        "SELECT DISTINCT charValue from $1 TYPE char"};
    for (final var query : queries) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(query);
        var rs = (SelectResults) q.execute(params);
        if (rs.size() < 1) {
          fail("testIteratorDefChar: Query fetched zero results ");
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
  }

  @Test
  public void testNonStaticInnerClassTypeDef() {
    Student.initializeCounter();
    var Arlist = new ArrayList();
    Arlist.add(new Student("asif"));
    Arlist.add(new Student("ketan"));

    var params = new Object[1];
    params[0] = Arlist;

    var queries = new String[] {

        "IMPORT org.apache.geode.cache.\"query\".data.Student;"
            + "IMPORT org.apache.geode.cache.\"query\".data.Student$Subject;"
            + "Select distinct * from  $1 as it1 ,  it1.subjects x  type Student$Subject  where x.subject='Hindi' ",
        "IMPORT org.apache.geode.cache.\"query\".data.Student;"
            + "IMPORT org.apache.geode.cache.\"query\".data.Student$Subject;"
            + "Select distinct * from  $1 as it1 ,  it1.subjects  type Student$Subject  where subject='Hindi' ",
        "IMPORT org.apache.geode.cache.\"query\".data.Student;"
            + "IMPORT org.apache.geode.cache.\"query\".data.Student$Subject;"
            + "Select distinct * from  $1 as it1 , (list<Student$Subject>) it1.subjects   where subject='Hindi' "};
    for (var i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        var rs = (SelectResults) q.execute(params);
        if (rs.size() != 1) {
          fail("testNonStaticInnerClassTypeDef: Query fetched results with size =" + rs.size()
              + " FOr Query number = " + (i + 1));
        }

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
  }

  @Test
  public void testStaticInnerClassTypeDef() {
    Student.initializeCounter();
    var Arlist = new ArrayList();
    Arlist.add(new Student("asif"));
    Arlist.add(new Student("ketan"));

    var params = new Object[1];
    params[0] = Arlist;

    var queries = new String[] {

        "IMPORT org.apache.geode.cache.\"query\".data.Student;"
            + "IMPORT org.apache.geode.cache.\"query\".data.Student$Teacher;"
            + "Select distinct * from  $1 as it1 ,  it1.teachers x  type Student$Teacher  where x.teacher='Y' ",
        "IMPORT org.apache.geode.cache.\"query\".data.Student;"
            + "IMPORT org.apache.geode.cache.\"query\".data.Student$Teacher;"
            + "Select distinct * from  $1 as it1 ,  it1.teachers  type Student$Teacher  where teacher='Y' ",
        "IMPORT org.apache.geode.cache.\"query\".data.Student;"
            + "IMPORT org.apache.geode.cache.\"query\".data.Student$Teacher;"
            + "Select distinct * from  $1 as it1 , (list<Student$Teacher>) it1.teachers  where teacher='Y' "};
    for (var i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        var rs = (SelectResults) q.execute(params);
        if (rs.size() != 1) {
          fail("testStaticInnerClassTypeDef: Query fetched results with size =" + rs.size()
              + " FOr Query number = " + (i + 1));
        }

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
  }
}
