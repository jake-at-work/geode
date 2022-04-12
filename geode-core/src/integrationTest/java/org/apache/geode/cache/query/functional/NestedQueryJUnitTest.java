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
 * NestedQueryJUnitTest.java JUnit based test
 *
 * Created on March 28, 2005, 11:35 AM
 */
package org.apache.geode.cache.query.functional;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ErrorCollector;

import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.internal.StructImpl;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class NestedQueryJUnitTest {

  private ObjectType resType1 = null;
  private ObjectType resType2 = null;

  private int resSize1 = 0;
  private int resSize2 = 0;

  private Iterator itert1 = null;
  private Iterator itert2 = null;

  private Set set1 = null;
  private Set set2 = null;

  private String s1;
  private String s2;

  @Rule
  public ErrorCollector errorCollector = new ErrorCollector(); // used by testQueries

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
    var r = CacheUtils.createRegion("Portfolios", Portfolio.class);
    for (var i = 0; i < 4; i++) {
      r.put(i + "", new Portfolio(i));
    }
  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  @Ignore("TODO: this test was disabled")
  @Test
  public void testQueries2() throws Exception {
    String queryString;
    Query query;
    Object result;
    // Executes successfully
    queryString =
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios WHERE NOT(SELECT DISTINCT * FROM positions.values p WHERE p.secId = 'IBM').isEmpty";
    query = CacheUtils.getQueryService().newQuery(queryString);
    result = query.execute();
    // Fails
    queryString =
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios where status = ELEMENT(SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios p where p.ID = 0).status";
    query = CacheUtils.getQueryService().newQuery(queryString);
    result = query.execute();
    // Executes successfully
    queryString =
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios x where status = ELEMENT(SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios p where x.ID = p.ID).status";
    query = CacheUtils.getQueryService().newQuery(queryString);
    result = query.execute();
    // Fails
    queryString =
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios x where status = ELEMENT(SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios p where p.ID = x.ID).status";
    query = CacheUtils.getQueryService().newQuery(queryString);
    result = query.execute();

    queryString =
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios x where status = ELEMENT(SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios p where p.ID = 0).status";
    query = CacheUtils.getQueryService().newQuery(queryString);
    result = query.execute();
  }

  @Test
  public void testQueries() throws Exception {
    var queries = new String[] {
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios WHERE NOT(SELECT DISTINCT * FROM positions.values p WHERE p.secId = 'IBM').isEmpty",
        "SELECT DISTINCT * FROM " + SEPARATOR + "Portfolios where NOT(SELECT DISTINCT * FROM "
            + SEPARATOR + "Portfolios p where p.ID = 0).isEmpty",
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios where status = ELEMENT(SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios p where p.ID = 0).status",
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios where status = ELEMENT(SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios p where p.ID = 0).status",
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios x where status = ELEMENT(SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios p where x.ID = p.ID).status",
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios x where status = ELEMENT(SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios p where p.ID = x.ID).status",
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios x where status = ELEMENT(SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios p where p.ID = 0).status"};
    for (final var s : queries) {
      try {
        var query = CacheUtils.getQueryService().newQuery(s);
        query.execute();
      } catch (Exception e) {
        errorCollector.addError(e);
      }
    }
  }

  @Test
  public void testNestedQueriesEvaluation() throws Exception {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    var queries = new String[] {
        "SELECT DISTINCT * FROM " + SEPARATOR + "Portfolios where NOT(SELECT DISTINCT * FROM "
            + SEPARATOR + "Portfolios p where p.ID = 0).isEmpty",
        // NQIU 1: PASS
        // "SELECT DISTINCT * FROM /Portfolios where status = ELEMENT(SELECT DISTINCT * FROM
        // /Portfolios p where ID = 0).status",
        // NQIU 2 : Failed: 16 May'05
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios x where status = ELEMENT(SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios p where p.ID = x.ID).status",
        // NQIU 3:PASS
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios x where status = ELEMENT(SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios p where p.ID = 0).status",
        // NQIU 4:PASS
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios WHERE NOT(SELECT DISTINCT * FROM positions.values p WHERE p.secId = 'IBM').isEmpty",
        // NQIU 5: PASS
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios x where status = ELEMENT(SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios p where x.ID = p.ID).status",
        // NQIU 6: PASS
    };
    var r = new SelectResults[queries.length][2];

    for (var i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);

        r[i][0] = (SelectResults) q.execute();
        var observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        if (!observer.isIndexesUsed) {
          CacheUtils.log("NO INDEX USED");
        }
        resType1 = (r[i][0]).getCollectionType().getElementType();
        resSize1 = ((r[i][0]).size());
        set1 = ((r[i][0]).asSet());
        // Iterator iter=set1.iterator();

      } catch (Exception e) {
        throw new AssertionError(q.getQueryString(), e);
      }
    }

    // Create an Index on status and execute the same query again.

    qs = CacheUtils.getQueryService();
    qs.createIndex("IDIndex", IndexType.FUNCTIONAL, "ID", SEPARATOR + "Portfolios pf");
    qs.createIndex("secIdIndex", IndexType.FUNCTIONAL, "b.secId",
        SEPARATOR + "Portfolios pf, pf.positions.values b");
    qs.createIndex("r1Index", IndexType.FUNCTIONAL, "secId",
        SEPARATOR + "Portfolios.values['0'].positions.values");

    for (var i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        var observer2 = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer2);
        r[i][1] = (SelectResults) q.execute();

        if (observer2.isIndexesUsed == true) {
        } else if (i != 3) {
          fail("Index not used");
        }
        resType2 = (r[i][1]).getCollectionType().getElementType();
        resSize2 = ((r[i][1]).size());
        set2 = ((r[i][1]).asSet());

      } catch (Exception e) {
        throw new AssertionError(q.getQueryString(), e);
      }
    }
    for (var j = 0; j <= 1; j++) {
      // Increase the value of j as you go on adding the Queries in queries[]
      if (((r[j][0]).getCollectionType().getElementType())
          .equals((r[j][1]).getCollectionType().getElementType())) {
        CacheUtils.log("Both Search Results are of the same Type i.e.--> "
            + (r[j][0]).getCollectionType().getElementType());
      } else {
        fail("FAILED:Search result Type is different in both the cases");
      }
      if ((r[j][0]).size() == (r[j][1]).size()) {
        CacheUtils.log("Both Search Results are of Same Size i.e.  Size= " + (r[j][1]).size());
      } else {
        fail("FAILED:Search result Type is different in both the cases");
      }
    }
    itert2 = set2.iterator();
    itert1 = set1.iterator();
    while (itert1.hasNext()) {
      var p1 = (Portfolio) itert1.next();
      var p2 = (Portfolio) itert2.next();
      if (!set1.contains(p2) || !set2.contains(p1)) {
        fail("FAILED: In both the Cases the members of ResultsSet are different.");
      }
    }
    CacheUtils.compareResultsOfWithAndWithoutIndex(r, this);
  }

  @Test
  public void testNestedQueriesResultsAsStructSet() throws Exception {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    var queries = new String[] {

        "SELECT DISTINCT * FROM" + " (SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios ptf, positions pos)"
            + " WHERE pos.value.secId = 'IBM'",
        "SELECT DISTINCT * FROM" + " (SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios AS ptf, positions AS pos)"
            + " WHERE pos.value.secId = 'IBM'",
        "SELECT DISTINCT * FROM" + " (SELECT DISTINCT * FROM ptf IN " + SEPARATOR
            + "Portfolios, pos IN positions)"
            + " WHERE pos.value.secId = 'IBM'",
        "SELECT DISTINCT * FROM"
            + " (SELECT DISTINCT pos AS myPos FROM " + SEPARATOR + "Portfolios ptf, positions pos)"
            + " WHERE myPos.value.secId = 'IBM'",
        "SELECT DISTINCT * FROM" + " (SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios ptf, positions pos) p"
            + " WHERE p.pos.value.secId = 'IBM'",
        "SELECT DISTINCT * FROM" + " (SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios ptf, positions pos) p"
            + " WHERE pos.value.secId = 'IBM'",
        "SELECT DISTINCT * FROM" + " (SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios, positions) p"
            + " WHERE p.positions.value.secId = 'IBM'",
        "SELECT DISTINCT * FROM" + " (SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios, positions)"
            + " WHERE positions.value.secId = 'IBM'",
        "SELECT DISTINCT * FROM" + " (SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios ptf, positions pos) p"
            + " WHERE p.get('pos').value.secId = 'IBM'",
        // NQIU 7: PASS
    };
    var r = new SelectResults[queries.length][2];


    for (var i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = (SelectResults) q.execute();
        var observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);

        resType1 = (r[i][0]).getCollectionType().getElementType();
        resSize1 = ((r[i][0]).size());
        set1 = ((r[i][0]).asSet());

      } catch (Exception e) {
        throw new AssertionError(e);
      }
    }

    // Create an Index on status and execute the same query again.

    qs = CacheUtils.getQueryService();
    qs.createIndex("secIdIndex", IndexType.FUNCTIONAL, "b.secId",
        SEPARATOR + "Portfolios pf, pf.positions.values b");
    for (var i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        r[i][1] = (SelectResults) q.execute();

        var observer2 = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer2);
        resType2 = (r[i][1]).getCollectionType().getElementType();
        resSize2 = ((r[i][1]).size());
        set2 = ((r[i][1]).asSet());

      } catch (Exception e) {
        throw new AssertionError(q.getQueryString(), e);
      }
    }
    for (var j = 0; j < queries.length; j++) {

      if (((r[j][0]).getCollectionType().getElementType())
          .equals((r[j][1]).getCollectionType().getElementType())) {
        CacheUtils.log("Both Search Results are of the same Type i.e.--> "
            + (r[j][0]).getCollectionType().getElementType());
      } else {
        fail("FAILED:Search result Type is different in both the cases");
      }
      if ((r[j][0]).size() == (r[j][1]).size()) {
        CacheUtils.log("Both Search Results are of Same Size i.e.  Size= " + (r[j][1]).size());
      } else {
        fail("FAILED:Search result Type is different in both the cases");
      }
    }
    var pass = true;
    itert1 = set1.iterator();
    while (itert1.hasNext()) {
      var p1 = (StructImpl) itert1.next();
      itert2 = set2.iterator();
      var found = false;
      while (itert2.hasNext()) {
        var p2 = (StructImpl) itert2.next();
        if ((p1).equals(p2)) {
          found = true;
        }
      }
      if (!found) {
        pass = false;
      }
    }
    if (!pass) {
      fail("Test failed");
    }

    CacheUtils.compareResultsOfWithAndWithoutIndex(r, this);
  }

  @Test
  public void testNestedQueryWithProjectionDoesNotReturnUndefinedForBug45131() throws Exception {
    var qs = CacheUtils.getQueryService();
    var region1 = CacheUtils.createRegion("portfolios1", Portfolio.class);
    var region2 = CacheUtils.createRegion("portfolios2", Portfolio.class);
    for (var i = 0; i <= 1000; i++) {
      var p = new Portfolio(i);
      p.createTime = i;
      region1.put(i, p);
      region2.put(i, p);
    }

    var p1IdIndex =
        qs.createIndex("P1IDIndex", IndexType.FUNCTIONAL, "P.ID", SEPARATOR + "portfolios1 P");
    var p2IdIndex =
        qs.createIndex("P2IDIndex", IndexType.FUNCTIONAL, "P2.ID", SEPARATOR + "portfolios2 P2");
    var createTimeIndex =
        qs.createIndex("createTimeIndex", IndexType.FUNCTIONAL, "P.createTime",
            SEPARATOR + "portfolios1 P");

    var results = (SelectResults) qs
        .newQuery(
            "SELECT P2.ID FROM " + SEPARATOR + "portfolios2 P2 where P2.ID in (SELECT P.ID from "
                + SEPARATOR + "portfolios1 P where P.createTime >= 500L and P.createTime < 1000L)")
        .execute();
    for (var o : results) {
      assertNotSame(o, QueryService.UNDEFINED);
    }
  }

  @Test
  public void testExecCacheForNestedQueries() throws Exception {
    var qs = CacheUtils.getQueryService();
    var region1 = CacheUtils.createRegion("portfolios1", Portfolio.class);
    var region2 = CacheUtils.createRegion("portfolios2", Portfolio.class);
    for (var i = 0; i <= 1000; i++) {
      var p = new Portfolio(i);
      p.createTime = i;
      region1.put(i, p);
      region2.put(i, p);
    }

    var p1IdIndex =
        qs.createIndex("P1IDIndex", IndexType.FUNCTIONAL, "P.ID", SEPARATOR + "portfolios1 P");
    var p2IdIndex =
        qs.createIndex("P2IDIndex", IndexType.FUNCTIONAL, "P2.ID", SEPARATOR + "portfolios2 P2");
    var createTimeIndex =
        qs.createIndex("createTimeIndex", IndexType.FUNCTIONAL, "P.createTime",
            SEPARATOR + "portfolios1 P");

    var rangeQueryString =
        "SELECT P.ID FROM " + SEPARATOR
            + "portfolios1 P WHERE P.createTime >= 500L AND P.createTime < 1000L";
    // Retrieve location ids that are within range
    var multiInnerQueryString =
        "SELECT P FROM " + SEPARATOR + "portfolios1 P WHERE P.ID IN(SELECT P2.ID FROM " + SEPARATOR
            + "portfolios2 P2 where P2.ID in ($1)) and P.createTime >=500L and P.createTime < 1000L";

    var rangeQuery = qs.newQuery(rangeQueryString);
    var rangeResults = (SelectResults) rangeQuery.execute();

    var multiInnerQuery = qs.newQuery(multiInnerQueryString);
    var params = new Object[1];
    params[0] = rangeResults;

    var results = (SelectResults) multiInnerQuery.execute(params);
    // By now we would have hit the ClassCastException, instead we just check
    // size and make sure we got results.
    assertEquals(500, results.size());
  }

  /**
   * Tests a nested query with shorts converted to integer types in the result set of the inner
   * query. The short field in the outer query should be evaluated against the integer types and
   * match.
   */
  @Test
  public void testNestedQueryWithShortTypesFromInnerQuery() throws Exception {
    var qs = CacheUtils.getQueryService();
    var region1 = CacheUtils.createRegion("portfolios1", Portfolio.class);
    var numEntries = 1000;
    var numIds = 100;
    for (var i = 0; i < numEntries; i++) {
      var p = new Portfolio(i % (numIds));
      p.shortID = (short) p.ID;
      region1.put("" + i, p);
    }

    helpTestIndexForQuery(
        "<trace>SELECT * FROM " + SEPARATOR
            + "portfolios1 p where p.shortID in (SELECT p.shortID FROM " + SEPARATOR
            + "portfolios1 p WHERE p.shortID = 1)",
        "p.shortID", SEPARATOR + "portfolios1 p");
  }

  /**
   * Tests a nested query that has duplicate results in the inner query Results should not be
   * duplicated in the final result set
   */
  @Test
  public void testNestedQueryWithMultipleMatchingResultsWithIn() throws Exception {
    var qs = CacheUtils.getQueryService();
    var region1 = CacheUtils.createRegion("portfolios1", Portfolio.class);
    var numEntries = 1000;
    var numIds = 100;
    for (var i = 0; i < numEntries; i++) {
      var p = new Portfolio(i % (numIds));
      p.shortID = (short) p.ID;
      region1.put("" + i, p);
    }

    helpTestIndexForQuery(
        "<trace>SELECT * FROM " + SEPARATOR + "portfolios1 p where p.ID in (SELECT p.ID FROM "
            + SEPARATOR + "portfolios1 p WHERE p.ID = 1)",
        "p.ID", SEPARATOR + "portfolios1 p");
  }

  /**
   * helper method to test against a compact range index
   */
  private void helpTestIndexForQuery(String query, String indexedExpression, String regionPath)
      throws Exception {
    var qs = CacheUtils.getQueryService();
    var observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);
    var nonIndexedResults = (SelectResults) qs.newQuery(query).execute();
    assertFalse(observer.isIndexesUsed);

    qs.createIndex("newIndex", indexedExpression, regionPath);
    var indexedResults = (SelectResults) qs.newQuery(query).execute();
    assertEquals(nonIndexedResults.size(), indexedResults.size());
    assertTrue(observer.isIndexesUsed);
  }

  private static class QueryObserverImpl extends QueryObserverAdapter {

    private boolean isIndexesUsed = false;
    private final ArrayList indexesUsed = new ArrayList();

    @Override
    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexesUsed.add(index.getName());
    }

    @Override
    public void afterIndexLookup(Collection results) {
      if (results != null) {
        isIndexesUsed = true;
      }
    }
  }
}
