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
package org.apache.geode.cache.query.functional;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexInvalidException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.Position;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;

public abstract class OrderByTestImplementation {

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();

  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  public abstract Region createRegion(String regionName, Class valueConstraint);

  public abstract Index createIndex(String indexName, IndexType indexType, String indexedExpression,
      String fromClause) throws IndexInvalidException, IndexNameConflictException,
      IndexExistsException, RegionNotFoundException, UnsupportedOperationException;

  public abstract Index createIndex(String indexName, String indexedExpression, String regionPath)
      throws IndexInvalidException, IndexNameConflictException, IndexExistsException,
      RegionNotFoundException, UnsupportedOperationException;

  public abstract boolean assertIndexUsedOnQueryNode();

  @Test
  public void testOrderByWithIndexResultDefaultProjection() throws Exception {
    var queries = new String[] {
        // Test case No. IUMR021
        "SELECT  distinct * FROM " + SEPARATOR + "portfolio1 pf1 where ID > 10 order by ID desc ",
        "SELECT  distinct * FROM " + SEPARATOR + "portfolio1 pf1 where ID > 10 order by ID asc ",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 and ID < 20 order by ID asc ",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 and ID < 20 order by ID desc ",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID desc ",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID asc",
        "SELECT  distinct * FROM " + SEPARATOR + "portfolio1 pf1 where ID != 10 order by ID asc ",
        "SELECT  distinct * FROM " + SEPARATOR + "portfolio1 pf1 where ID != 10 order by ID desc ",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 order by ID desc limit 5",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 order by ID asc limit 5",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 and ID < 20 order by ID asc limit 5 ",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 and ID < 20 order by ID desc limit 5",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID desc limit 5",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID asc limit 5",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID != 10 order by ID asc limit 10",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID != 10 order by ID desc limit 10",

    };
    var r = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    var r1 = createRegion("portfolio1", Portfolio.class);

    for (var i = 0; i < 50; i++) {
      r1.put(i + "", new Portfolio(i));
    }


    // Execute Queries without Indexes
    for (var i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes

    createIndex("IDIndexPf1", IndexType.FUNCTIONAL, "ID", SEPARATOR + "portfolio1");

    // Execute Queries with Indexes
    for (var i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        var observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        var rcw = (SelectResults) r[i][1];
        var indexLimit = queries[i].indexOf("limit");
        var limit = -1;
        var limitQuery = indexLimit != -1;
        if (limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit + 5).trim());
        }
        assertTrue("Result size is " + rcw.size() + " and limit is " + limit,
            !limitQuery || rcw.size() <= limit);
        var colType = rcw.getCollectionType().getSimpleClassName();
        if (!(colType.equals("Ordered") || colType.equals("LinkedHashSet"))) {
          fail("The collection type " + colType + " is not expexted");
        }
        if (assertIndexUsedOnQueryNode() && !observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }

        var itr = observer.indexesUsed.iterator();
        while (itr.hasNext()) {
          if (!(itr.next().toString()).equals("IDIndexPf1")) {
            fail("<IDIndexPf1> was expected but found " + itr.next().toString());
          }
          // assertIndexDetailsEquals("statusIndexPf1",itr.next().toString());
        }

        var indxs = observer.indexesUsed.size();

        System.out.println("**************************************************Indexes Used :::::: "
            + indxs + " Index Name: " + observer.indexName);

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    var ssOrrs = new StructSetOrResultsSet();

    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, true, queries);
    ssOrrs.compareExternallySortedQueriesWithOrderBy(queries, r);
  }

  @Test
  public void testOrderByWithColumnAlias_Bug52041_1() throws Exception {

    var region = createRegion("portfolio", Portfolio.class);
    for (var i = 1; i < 200; ++i) {
      var pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      pf.status = "active";
      region.put("" + i, pf);
    }
    var queryStr =
        "select distinct p.status, p.shortID as short_id  from " + SEPARATOR
            + "portfolio p where p.ID >= 0 "
            + "order by short_id asc";
    var qs = CacheUtils.getQueryService();
    var query = qs.newQuery(queryStr);
    var results = (SelectResults<Struct>) query.execute();
    var iter = results.asList().iterator();
    var counter = 0;
    while (iter.hasNext()) {
      var str = iter.next();
      assertEquals(counter, ((Short) str.get("short_id")).intValue());
      ++counter;
    }
    assertEquals(39, counter - 1);
    var cs = ((DefaultQuery) query).getSimpleSelect();
    var orderbyAtts = cs.getOrderByAttrs();
    assertEquals(orderbyAtts.get(0).getColumnIndex(), 1);

  }

  @Test
  public void testOrderByWithColumnAlias_Bug52041_2() throws Exception {

    var region = createRegion("portfolio", Portfolio.class);
    for (var i = 0; i < 200; ++i) {
      var pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      pf.status = "active";
      region.put("" + i, pf);
    }
    var queryStr =
        "select distinct p.ID as _id, p.shortID as short_id  from " + SEPARATOR
            + "portfolio p where p.ID >= 0 "
            + "order by short_id asc, p.ID desc";
    var qs = CacheUtils.getQueryService();
    var query = qs.newQuery(queryStr);
    var results = (SelectResults<Struct>) query.execute();
    var iter = results.asList().iterator();
    var counter = 0;
    var k = 0;
    while (iter.hasNext()) {
      k = ((counter) / 5 + 1) * 5 - 1;
      var str = iter.next();
      assertEquals(counter / 5, ((Short) str.get("short_id")).intValue());
      assertEquals(k - (counter) % 5, ((Integer) str.get("_id")).intValue());
      ++counter;
    }
    var cs = ((DefaultQuery) query).getSimpleSelect();
    var orderbyAtts = cs.getOrderByAttrs();
    assertEquals(orderbyAtts.get(0).getColumnIndex(), 1);
    assertEquals(orderbyAtts.get(1).getColumnIndex(), 0);

  }

  @Test
  public void testOrderByWithIndexResultWithProjection() throws Exception {
    var queries = new String[] {
        // Test case No. IUMR021
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 order by ID desc ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 order by ID asc ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 and ID < 20 order by ID asc ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 and ID < 20 order by ID desc ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID desc ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID asc",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID != 10 order by ID asc ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID != 10 order by ID desc ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 order by ID desc limit 5",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 order by ID asc limit 5",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 and ID < 20 order by ID asc limit 5 ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 and ID < 20 order by ID desc limit 5",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID desc limit 5",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID asc limit 5",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID != 10 order by ID asc limit 10",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID != 10 order by ID desc limit 10",

    };
    var r = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    var r1 = createRegion("portfolio1", Portfolio.class);

    for (var i = 0; i < 50; i++) {
      r1.put(i + "", new Portfolio(i));
    }


    // Execute Queries without Indexes
    for (var i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes

    createIndex("IDIndexPf1", IndexType.FUNCTIONAL, "ID", SEPARATOR + "portfolio1");

    // Execute Queries with Indexes
    for (var i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        var observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        var rcw = (SelectResults) r[i][1];
        var indexLimit = queries[i].indexOf("limit");
        var limit = -1;
        var limitQuery = indexLimit != -1;
        if (limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit + 5).trim());
        }
        assertTrue(!limitQuery || rcw.size() <= limit);
        // assertIndexDetailsEquals("Set",rcw.getCollectionType().getSimpleClassName());
        var colType = rcw.getCollectionType().getSimpleClassName();
        if (!(colType.equals("Ordered") || colType.equals("LinkedHashSet"))) {
          fail("The collection type " + colType + " is not expexted");
        }
        if (assertIndexUsedOnQueryNode() && !observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }

        var itr = observer.indexesUsed.iterator();
        while (itr.hasNext()) {
          if (!(itr.next().toString()).equals("IDIndexPf1")) {
            fail("<IDIndexPf1> was expected but found " + itr.next().toString());
          }
          // assertIndexDetailsEquals("statusIndexPf1",itr.next().toString());
        }

        var indxs = observer.indexesUsed.size();

        System.out.println("**************************************************Indexes Used :::::: "
            + indxs + " Index Name: " + observer.indexName);

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    var ssOrrs = new StructSetOrResultsSet();

    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, true, queries);
    ssOrrs.compareExternallySortedQueriesWithOrderBy(queries, r);
  }

  @Test
  public void testMultiColOrderByWithIndexResultDefaultProjection() throws Exception {
    var queries = new String[] {
        // Test case No. IUMR021
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 order by ID desc, pkid asc ",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 order by ID asc, pkid desc ",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 and ID < 20 order by ID asc, pkid asc ",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 and ID < 20 order by ID desc , pkid desc",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID desc, pkid desc ",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID asc, pkid asc",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID != 10 order by ID asc, pkid asc ",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID != 10 order by ID desc, pkid desc ",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 order by ID desc, pkid asc limit 5",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 order by ID asc, pkid desc limit 5",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 and ID < 20 order by ID asc, pkid asc limit 5 ",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 and ID < 20 order by ID desc, pkid desc limit 5",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID desc, pkid asc limit 5",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID asc, pkid desc limit 5",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID != 10 order by ID asc, pkid asc limit 10",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where ID != 10 order by ID desc, pkid desc limit 10",

    };
    var r = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    var r1 = createRegion("portfolio1", Portfolio.class);

    for (var i = 0; i < 50; i++) {
      r1.put(i + "", new Portfolio(i));
    }


    // Execute Queries without Indexes
    for (var i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes

    createIndex("IDIndexPf1", IndexType.FUNCTIONAL, "ID", SEPARATOR + "portfolio1");

    // Execute Queries with Indexes
    for (var i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        var observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        var rcw = (SelectResults) r[i][1];
        var indexLimit = queries[i].indexOf("limit");
        var limit = -1;
        var limitQuery = indexLimit != -1;
        if (limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit + 5).trim());
        }
        assertTrue(!limitQuery || rcw.size() <= limit);
        assertEquals("Ordered", rcw.getCollectionType().getSimpleClassName());
        if (assertIndexUsedOnQueryNode() && !observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }

        var itr = observer.indexesUsed.iterator();
        while (itr.hasNext()) {
          if (!(itr.next().toString()).equals("IDIndexPf1")) {
            fail("<IDIndexPf1> was expected but found " + itr.next().toString());
          }
          // assertIndexDetailsEquals("statusIndexPf1",itr.next().toString());
        }

        var indxs = observer.indexesUsed.size();

        System.out.println("**************************************************Indexes Used :::::: "
            + indxs + " Index Name: " + observer.indexName);

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    var ssOrrs = new StructSetOrResultsSet();

    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, true, queries);
    ssOrrs.compareExternallySortedQueriesWithOrderBy(queries, r);
  }

  public abstract String[] getQueriesForMultiColOrderByWithIndexResultWithProjection();

  @Test
  public void testMultiColOrderByWithIndexResultWithProjection() throws Exception {
    var queries = getQueriesForMultiColOrderByWithIndexResultWithProjection();
    var r = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    var r1 = createRegion("portfolio1", Portfolio.class);

    for (var i = 0; i < 50; i++) {
      r1.put(i + "", new Portfolio(i));
    }


    // Execute Queries without Indexes
    for (var i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes

    createIndex("IDIndexPf1", IndexType.FUNCTIONAL, "ID", SEPARATOR + "portfolio1");

    // Execute Queries with Indexes
    for (var i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        var observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        var rcw = (SelectResults) r[i][1];
        var indexLimit = queries[i].indexOf("limit");
        var limit = -1;
        var limitQuery = indexLimit != -1;
        if (limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit + 5).trim());
        }
        assertTrue(!limitQuery || rcw.size() <= limit);
        assertEquals("Ordered", rcw.getCollectionType().getSimpleClassName());
        if (assertIndexUsedOnQueryNode() && !observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }

        var itr = observer.indexesUsed.iterator();
        while (itr.hasNext()) {
          if (!(itr.next().toString()).equals("IDIndexPf1")) {
            fail("<IDIndexPf1> was expected but found " + itr.next().toString());
          }
          // assertIndexDetailsEquals("statusIndexPf1",itr.next().toString());
        }

        var indxs = observer.indexesUsed.size();

        System.out.println("**************************************************Indexes Used :::::: "
            + indxs + " Index Name: " + observer.indexName);

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    var ssOrrs = new StructSetOrResultsSet();

    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, true, queries);
    ssOrrs.compareExternallySortedQueriesWithOrderBy(queries, r);
  }

  @Test
  public void testMultiColOrderByWithMultiIndexResultDefaultProjection() throws Exception {
    var queries = new String[] {
        // Test case No. IUMR021
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid = '12' and ID > 10 order by ID desc, pkid asc ",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '1' and ID > 10 order by ID asc, pkid desc ",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid = '13'and  ID > 10 and ID < 20 order by ID asc, pkid asc ",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid <'9' and ID > 10 and ID < 20 order by ID desc , pkid desc",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid = '15' and ID >= 10 and ID <= 20 order by ID desc, pkid desc ",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '1' and pkid <='9' and ID >= 10 and ID <= 20 order by ID asc, pkid asc",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '1' and ID != 10 order by ID asc, pkid asc ",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '1' and ID != 10 order by ID desc, pkid desc ",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid = '17' and ID > 10 order by ID desc, pkid asc limit 5",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '17' and ID > 10 order by ID asc, pkid desc limit 5",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid < '7' and ID > 10 and ID < 20 order by ID asc, pkid asc limit 5 ",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid = '18' and ID > 10 and ID < 20 order by ID desc, pkid desc limit 5",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '1' and ID >= 10 and ID <= 20 order by ID desc, pkid asc limit 5",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid != '17' and ID >= 10 and ID <= 20 order by ID asc, pkid desc limit 5",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '0' and ID != 10 order by ID asc, pkid asc limit 10",
        "SELECT  distinct * FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '1' and ID != 10 order by ID desc, pkid desc limit 10",

    };
    var r = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    var r1 = createRegion("portfolio1", Portfolio.class);

    for (var i = 0; i < 50; i++) {
      r1.put(i + "", new Portfolio(i));
    }


    // Execute Queries without Indexes
    for (var i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes

    createIndex("IDIndexPf1", IndexType.FUNCTIONAL, "ID", SEPARATOR + "portfolio1");
    createIndex("PKIDIndexPf1", IndexType.FUNCTIONAL, "pkid", SEPARATOR + "portfolio1");
    // Execute Queries with Indexes
    for (var i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        var observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();

        var rcw = (SelectResults) r[i][1];

        var indexLimit = queries[i].indexOf("limit");
        var limit = -1;
        var limitQuery = indexLimit != -1;
        if (limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit + 5).trim());
        }
        assertTrue(!limitQuery || rcw.size() <= limit);
        assertEquals("Ordered", rcw.getCollectionType().getSimpleClassName());
        if (assertIndexUsedOnQueryNode() && !observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }

        for (final var o : observer.indexesUsed) {
          var indexUsed = o.toString();
          if (!(indexUsed).equals("IDIndexPf1")) {
            fail("<IDIndexPf1> was expected but found " + indexUsed);
          }
          // assertIndexDetailsEquals("statusIndexPf1",itr.next().toString());
        }

        var indxs = observer.indexesUsed.size();

        System.out.println("**************************************************Indexes Used :::::: "
            + indxs + " Index Name: " + observer.indexName);

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    var ssOrrs = new StructSetOrResultsSet();

    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, true, queries);
    ssOrrs.compareExternallySortedQueriesWithOrderBy(queries, r);
  }

  public abstract String[] getQueriesForMultiColOrderByWithMultiIndexResultProjection();

  @Test
  public void testMultiColOrderByWithMultiIndexResultProjection() throws Exception {
    var queries = getQueriesForMultiColOrderByWithMultiIndexResultProjection();
    var r = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    var r1 = createRegion("portfolio1", Portfolio.class);

    for (var i = 0; i < 50; i++) {
      r1.put(i + "", new Portfolio(i));
    }


    // Execute Queries without Indexes
    for (var i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes

    createIndex("IDIndexPf1", IndexType.FUNCTIONAL, "ID", SEPARATOR + "portfolio1");
    createIndex("PKIDIndexPf1", IndexType.FUNCTIONAL, "pkid", SEPARATOR + "portfolio1");
    // Execute Queries with Indexes
    for (var i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        var observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        var indexLimit = queries[i].indexOf("limit");
        var limit = -1;
        var limitQuery = indexLimit != -1;
        if (limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit + 5).trim());
        }
        var rcw = (SelectResults) r[i][1];
        assertEquals("Ordered", rcw.getCollectionType().getSimpleClassName());
        if (assertIndexUsedOnQueryNode() && !observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }
        assertTrue(!limitQuery || rcw.size() <= limit);

        for (final var o : observer.indexesUsed) {
          var indexUsed = o.toString();
          if (!(indexUsed).equals("IDIndexPf1")) {
            fail("<IDIndexPf1> was expected but found " + indexUsed);
          }
          // assertIndexDetailsEquals("statusIndexPf1",itr.next().toString());
        }

        var indxs = observer.indexesUsed.size();

        System.out.println("**************************************************Indexes Used :::::: "
            + indxs + " Index Name: " + observer.indexName);

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    var ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, true, queries);
    ssOrrs.compareExternallySortedQueriesWithOrderBy(queries, r);
  }

  public abstract String[] getQueriesForLimitNotAppliedIfOrderByNotUsingIndex();

  @Test
  public void testLimitNotAppliedIfOrderByNotUsingIndex() throws Exception {
    var queries = getQueriesForLimitNotAppliedIfOrderByNotUsingIndex();

    var r = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    var r1 = createRegion("portfolio1", Portfolio.class);

    for (var i = 0; i < 50; i++) {
      r1.put(i + "", new Portfolio(i));
    }


    // Execute Queries without Indexes
    for (var i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes

    createIndex("PKIDIndexPf1", IndexType.FUNCTIONAL, "pkid", SEPARATOR + "portfolio1");
    // Execute Queries with Indexes
    for (var i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        var observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        var indexLimit = queries[i].indexOf("limit");
        var limit = -1;
        var limitQuery = indexLimit != -1;
        if (limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit + 5).trim());
        }

        var rcw = (SelectResults) r[i][1];
        assertEquals("Ordered", rcw.getCollectionType().getSimpleClassName());
        if (assertIndexUsedOnQueryNode() && !observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }
        assertTrue(!limitQuery || !observer.limitAppliedAtIndex);

        for (final var o : observer.indexesUsed) {
          var indexUsed = o.toString();
          if (!(indexUsed).equals("PKIDIndexPf1")) {
            fail("<PKIDIndexPf1> was expected but found " + indexUsed);
          }
          // assertIndexDetailsEquals("statusIndexPf1",itr.next().toString());
        }

        var indxs = observer.indexesUsed.size();

        System.out.println("**************************************************Indexes Used :::::: "
            + indxs + " Index Name: " + observer.indexName);

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    var ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, true, queries);
    ssOrrs.compareExternallySortedQueriesWithOrderBy(queries, r);
  }



  protected abstract String[] getQueriesForOrderByWithNullValues();

  @Test
  public void testOrderByWithNullValuesUseIndex() throws Exception {
    // IN ORDER BY NULL values are treated as smallest. E.g For an ascending order by field
    // its null values are reported first and then the values in ascending order.
    var queries =
        new String[] {
            "SELECT  distinct * FROM " + SEPARATOR + "portfolio1 pf1 where ID > 0 order by pkid", // 0
            // null
            // values
            // are
            // first
            // in
            // the
            // order.
            "SELECT  distinct * FROM " + SEPARATOR
                + "portfolio1 pf1 where ID > 0 order by pkid asc", // 1 same as
            // above.
            "SELECT  distinct * FROM " + SEPARATOR + "portfolio1 where ID > 0 order by pkid desc", // 2
            // null
            // values
            // are
            // last in the order.
            "SELECT  distinct pkid FROM " + SEPARATOR + "portfolio1 pf1 where ID > 0 order by pkid", // 3
            // null
            // values
            // are first in the
            // order.
            "SELECT  distinct pkid FROM " + SEPARATOR
                + "portfolio1 pf1 where ID > 0 order by pkid asc", // 4
            "SELECT  distinct pkid FROM " + SEPARATOR
                + "portfolio1 pf1 where ID > 0 order by pkid desc", // 5 null
            // values are
            // last in the
            // order.
            "SELECT  distinct ID, pkid FROM " + SEPARATOR
                + "portfolio1 pf1 where ID < 1000 order by pkid", // 6
            "SELECT  distinct ID, pkid FROM " + SEPARATOR
                + "portfolio1 pf1 where ID > 3 order by pkid", // 7
            "SELECT  distinct ID, pkid FROM " + SEPARATOR
                + "portfolio1 pf1 where ID < 1000 order by pkid", // 8
            "SELECT  distinct ID, pkid FROM " + SEPARATOR
                + "portfolio1 pf1 where ID > 0 order by pkid", // 9
        };

    var r = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();

    // Create Regions
    final var size = 9;
    final var numNullValues = 3;
    var r1 = createRegion("portfolio1", Portfolio.class);
    for (var i = 1; i <= size; i++) {
      var pf = new Portfolio(i);
      // Add numNullValues null values.
      if (i <= numNullValues) {
        pf.pkid = null;
        pf.status = "a" + i;
      }
      r1.put(i + "", pf);
    }

    // Create Indexes
    createIndex("IDIndexPf1", IndexType.FUNCTIONAL, "ID", SEPARATOR + "portfolio1");
    createIndex("PKIDIndexPf1", IndexType.FUNCTIONAL, "pkid", SEPARATOR + "portfolio1");

    Query q = null;
    SelectResults results = null;
    List list = null;
    var str = "";
    try {
      // Query 0 - null values are first in the order.
      var observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);

      str = queries[0];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);

      results = (SelectResults) q.execute();

      if (assertIndexUsedOnQueryNode() && !observer.isIndexesUsed) {
        fail("Index is NOT uesd");
      }

      r[0][0] = results;
      list = results.asList();
      for (var i = 1; i <= size; i++) {
        var p = (Portfolio) list.get((i - 1));
        if (i <= numNullValues) {
          assertNull("Expected null value for pkid, p: " + p, p.pkid);
        } else {
          assertNotNull("Expected not null value for pkid", p.pkid);
          if (!p.pkid.equals("" + i)) {
            fail(" Value of pkid is not in expected order.");
          }
        }
      }

      // Query 1 - null values are first in the order.
      str = queries[1];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults) q.execute();
      list = results.asList();
      for (var i = 1; i <= size; i++) {
        var p = (Portfolio) list.get((i - 1));
        if (i <= numNullValues) {
          assertNull("Expected null value for pkid", p.pkid);
        } else {
          assertNotNull("Expected not null value for pkid", p.pkid);
          if (!p.pkid.equals("" + i)) {
            fail(" Value of pkid is not in expected order.");
          }
        }
      }

      // Query 2 - null values are last in the order.
      str = queries[2];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults) q.execute();
      list = results.asList();
      for (var i = 1; i <= size; i++) {
        var p = (Portfolio) list.get((i - 1));
        if (i > (size - numNullValues)) {
          assertNull("Expected null value for pkid", p.pkid);
        } else {
          assertNotNull("Expected not null value for pkid", p.pkid);
          if (!p.pkid.equals("" + (size - (i - 1)))) {
            fail(" Value of pkid is not in expected order.");
          }
        }
      }

      // Query 3 - 1 distinct null value with pkid.
      str = queries[3];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults) q.execute();
      list = results.asList();
      for (var i = 1; i <= list.size(); i++) {
        var pkid = (String) list.get((i - 1));
        if (i == 1) {
          assertNull("Expected null value for pkid", pkid);
        } else {
          assertNotNull("Expected not null value for pkid", pkid);
          if (!pkid.equals("" + (numNullValues + (i - 1)))) {
            fail(" Value of pkid is not in expected order.");
          }
        }
      }

      // Query 4 - 1 distinct null value with pkid.
      observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);

      str = queries[4];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults) q.execute();

      if (assertIndexUsedOnQueryNode() && !observer.isIndexesUsed) {
        fail("Index is NOT uesd");
      }

      list = results.asList();
      for (var i = 1; i <= list.size(); i++) {
        var pkid = (String) list.get((i - 1));
        if (i == 1) {
          assertNull("Expected null value for pkid", pkid);
        } else {
          assertNotNull("Expected not null value for pkid", pkid);
          if (!pkid.equals("" + (numNullValues + (i - 1)))) {
            fail(" Value of pkid is not in expected order.");
          }
        }
      }

      // Query 5 - 1 distinct null value with pkid at the end.
      str = queries[5];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults) q.execute();
      list = results.asList();
      for (var i = 1; i <= list.size(); i++) {
        var pkid = (String) list.get((i - 1));
        if (i == (list.size())) {
          assertNull("Expected null value for pkid", pkid);
        } else {
          assertNotNull("Expected not null value for pkid", pkid);
          if (!pkid.equals("" + (size - (i - 1)))) {
            fail(" Value of pkid is not in expected order.");
          }
        }
      }

      // Query 6 - ID field values should be in the same order.
      str = queries[6];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults) q.execute();
      list = results.asList();
      for (var i = 1; i <= size; i++) {
        var strct = (Struct) list.get(i - 1);
        int id = (Integer) strct.getFieldValues()[0];
        // ID should be one of 1, 2, 3 because of distinct
        if (i <= numNullValues) {
          if (!(id == 1 || id == 2 || id == 3)) {
            fail(" Value of ID is not as expected " + id);
          }
        } else {
          if (id != i) {
            fail(" Value of ID is not as expected " + id);
          }
        }
      }

      // Query 7 - ID field values should be in the same order.
      str = queries[7];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults) q.execute();
      list = results.asList();
      for (var i = 1; i <= list.size(); i++) {
        var strct = (Struct) list.get(i - 1);
        int id = (Integer) strct.getFieldValues()[0];
        if (id != (numNullValues + i)) {
          fail(" Value of ID is not as expected, " + id);
        }
      }

      // Query 8 - ID, pkid field values should be in the same order.
      str = queries[8];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults) q.execute();
      list = results.asList();
      for (var i = 1; i <= size; i++) {
        var vals = (Struct) list.get((i - 1));
        int id = (Integer) vals.get("ID");
        var pkid = (String) vals.get("pkid");

        // ID should be one of 1, 2, 3 because of distinct
        if (i <= numNullValues) {
          if (!(id == 1 || id == 2 || id == 3)) {
            fail(" Value of ID is not as expected " + id);
          }
          assertNull("Expected null value for pkid", pkid);
        } else {
          if (id != i) {
            fail(" Value of ID is not as expected " + id);
          }
          assertNotNull("Expected not null value for pkid", pkid);
          if (!pkid.equals("" + i)) {
            fail(" Value of pkid is not in expected order.");
          }
        }
      }

      // Query 9 - ID, pkid field values should be in the same order.
      str = queries[9];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults) q.execute();
      list = results.asList();

      for (var i = 1; i <= list.size(); i++) {
        var vals = (Struct) list.get((i - 1));
        int id = (Integer) vals.get("ID");
        var pkid = (String) vals.get("pkid");

        if (i <= numNullValues) {
          assertNull("Expected null value for pkid, " + pkid, pkid);
          if (!(id == 1 || id == 2 || id == 3)) {
            fail(" Value of ID is not as expected " + id);
          }
        } else {
          if (!pkid.equals("" + i)) {
            fail(" Value of pkid is not as expected, " + pkid);
          }
          if (id != i) {
            fail(" Value of ID is not as expected, " + id);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(q.getQueryString());
    }

  }

  @Test
  public void testOrderByForUndefined() throws Exception {
    var queries =
        new String[] {
            "SELECT DISTINCT position1.secId FROM " + SEPARATOR + "test ORDER BY position1.secId", // 0
            "SELECT DISTINCT position1.secId FROM " + SEPARATOR
                + "test ORDER BY position1.secId desc", // 1
            "SELECT DISTINCT position1.secId FROM " + SEPARATOR
                + "test where ID > 0  ORDER BY position1.secId", // 2
            "SELECT DISTINCT position1.secId FROM " + SEPARATOR
                + "test where ID > 0  ORDER BY position1.secId desc", // 3
            "SELECT DISTINCT position1.secId, ID FROM " + SEPARATOR
                + "test ORDER BY position1.secId, ID", // 4
            "SELECT DISTINCT position1.secId, ID FROM " + SEPARATOR
                + "test ORDER BY position1.secId desc, ID",// 5
        };
    var r1 = createRegion("test", Portfolio.class);
    for (var i = 0; i < 10; i++) {
      var pf = new Portfolio(i);
      if (i % 2 == 0) {
        pf.position1 = null;
      }
      r1.put(i + "", pf);
    }
    var qs = CacheUtils.getQueryService();
    var sr = new SelectResults[queries.length][2];
    Object[] srArr = null;
    for (var i = 0; i < queries.length; i++) {
      try {
        sr[i][0] = (SelectResults) qs.newQuery(queries[i]).execute();
        srArr = sr[i][0].toArray();
        if (i == 0) {
          assertEquals("First result should be undefined for query " + queries[i],
              QueryService.UNDEFINED, srArr[0]);
        } else if (i == 1) {
          assertEquals("Last result should be undefined for query " + queries[i],
              QueryService.UNDEFINED, srArr[srArr.length - 1]);
        } else if (i == 2) {
          assertEquals("First result should be undefined for query " + queries[i],
              QueryService.UNDEFINED, srArr[0]);
        } else if (i == 3) {
          assertEquals("Last result should be undefined for query " + queries[i],
              QueryService.UNDEFINED, srArr[srArr.length - 1]);
        } else if (i == 4) {
          for (var j = 0; j < srArr.length / 2; j++) {
            assertEquals("Undefined should  have been returned for query " + queries[i],
                QueryService.UNDEFINED, ((Struct) srArr[j]).getFieldValues()[0]);
          }
        } else if (i == 5) {
          for (var j = srArr.length - 1; j > srArr.length / 2; j--) {
            assertEquals("Undefined should  have been returned for query " + queries[i],
                QueryService.UNDEFINED, ((Struct) srArr[j]).getFieldValues()[0]);
          }
        }
      } catch (Exception e) {
        fail("Query execution failed for: " + queries[i] + " : " + e);
      }
    }


    createIndex("secIndex", "position1.secId", r1.getFullPath());
    createIndex("IDIndex", "ID", r1.getFullPath());

    for (var i = 0; i < queries.length; i++) {
      try {
        sr[i][1] = (SelectResults) qs.newQuery(queries[i]).execute();
        srArr = sr[i][1].toArray();
        if (i == 0) {
          assertEquals("First result should be undefined for query " + queries[i],
              QueryService.UNDEFINED, srArr[0]);
        } else if (i == 1) {
          assertEquals("Last result should be undefined for query " + queries[i],
              QueryService.UNDEFINED, srArr[srArr.length - 1]);
        } else if (i == 2) {
          assertEquals("First result should be undefined for query " + queries[i],
              QueryService.UNDEFINED, srArr[0]);
        } else if (i == 3) {
          assertEquals("Last result should be undefined for query " + queries[i],
              QueryService.UNDEFINED, srArr[srArr.length - 1]);
        } else if (i == 4) {
          for (var j = 0; j < srArr.length / 2; j++) {
            assertEquals("Undefined should  have been returned for query " + queries[i],
                QueryService.UNDEFINED, ((Struct) srArr[j]).getFieldValues()[0]);
          }
        } else if (i == 5) {
          for (var j = srArr.length - 1; j > srArr.length / 2; j--) {
            assertEquals("Undefined should  have been returned for query " + queries[i],
                QueryService.UNDEFINED, ((Struct) srArr[j]).getFieldValues()[0]);
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail("Query execution failed for: " + queries[i] + " : " + e);
      }
    }

    CacheUtils.compareResultsOfWithAndWithoutIndex(sr);
  }

  class QueryObserverImpl extends QueryObserverAdapter {
    boolean isIndexesUsed = false;

    ArrayList indexesUsed = new ArrayList();

    String indexName;
    boolean limitAppliedAtIndex = false;

    @Override
    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexName = index.getName();
      indexesUsed.add(index.getName());
    }

    @Override
    public void afterIndexLookup(Collection results) {
      if (results != null) {
        isIndexesUsed = true;
      }
    }

    @Override
    public void limitAppliedAtIndexLevel(Index index, int limit, Collection indexResult) {
      limitAppliedAtIndex = true;
    }

  }


}
