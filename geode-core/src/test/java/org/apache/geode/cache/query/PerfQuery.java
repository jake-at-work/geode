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
package org.apache.geode.cache.query;

import static org.apache.geode.cache.Region.SEPARATOR;

import java.text.DateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Set;

import it.unimi.dsi.fastutil.floats.FloatArrayList;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.ResultsSet;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.NanoTimer;

public class PerfQuery {
  private static final int NUM_ITERATIONS = 20000;

  // exec types
  private static final int HAND_CODED = 0;
  private static final int BRUTE_FORCE = 1;
  private static final int INDEXED = 2;
  private static final int INDEX_CREATE = 3;
  private static final String[] execTypeStrings =
      new String[] {"hand-coded", "brute force", "indexed", "index-create"};
  private static final int[] DATA_SET_SIZES = new int[] {100, 1000, 10000, 20000};


  private DistributedSystem ds;
  private Region region = null;
  private RegionAttributes regionAttributes;
  private QueryService qs;
  protected Cache cache;

  // RESULTS
  private final FloatArrayList[] results;


  /** Creates a new instance of PerfQuery */
  public PerfQuery() {
    results = new FloatArrayList[4];
    results[BRUTE_FORCE] = new FloatArrayList();
    results[HAND_CODED] = new FloatArrayList();
    results[INDEXED] = new FloatArrayList();
    results[INDEX_CREATE] = new FloatArrayList();
  }

  public void run() throws Exception {
    var startTime = new Date();
    setUp();
    var formatter = DateFormat.getDateTimeInstance();
    System.out.println("Test started at: " + formatter.format(startTime));
    runQueries();
    printSummary();
    tearDown();
    var endTime = new Date();
    System.out.println("Test ended at: " + formatter.format(endTime));
    var durationMs = endTime.getTime() - startTime.getTime();
    var durationS = durationMs / 1000;
    var durationM = durationS / 60;
    var durationMM = durationM % 60;
    var durationH = durationM / 60;
    System.out.println("Test took " + durationH + "hrs, " + durationMM + "min.");
  }

  private void printSummary() {
    System.out.println("Query Execution Performance Results Summary");
    System.out.println("num iterations = " + NUM_ITERATIONS);
    System.out.println();
    System.out.println("Average query execution time in ms");
    System.out.println();

    var setNames = new String[] {"33% Retrieval", "0% Retrieval"};
    for (var setI = 0; setI < setNames.length; setI++) {
      var setName = setNames[setI];
      System.out.println(setName + ":");
      System.out.println("dataset size,hand-coded,brute-force,indexed,[index-create-time]");

      for (var szi = 0; szi < DATA_SET_SIZES.length; szi++) {
        System.out.print(DATA_SET_SIZES[szi]);
        System.out.print(',');
        for (var ti = HAND_CODED; ti <= INDEX_CREATE; ti++) {

          // skip over first set of each type, which was warm up
          var ix = ((setI + 1) * DATA_SET_SIZES.length) + szi;

          System.out.print(results[ti].get(ix));
          if (ti < INDEX_CREATE) {
            System.out.print(',');
          }
        }
        System.out.println();
      }
      System.out.println();
    }
  }

  private void runQueries() throws Exception {
    String queryString;
    Query query;

    // WARM-UP
    System.out.println("WARMING UP...");
    queryString = "select distinct * from " + SEPARATOR + "portfolios where type = 'type1'";
    query = qs.newQuery(queryString);
    runQuery(getType1HandQuery(queryString), HAND_CODED);
    runQuery(query, BRUTE_FORCE);
    runQuery(query, INDEXED);

    warmUpIndexCreation();

    System.out.println("END WARM UP");

    // 1/3 DATASET
    queryString = "select distinct * from " + SEPARATOR + "portfolios where type = 'type1'";
    query = qs.newQuery(queryString);
    runQuery(getType1HandQuery(queryString), HAND_CODED);
    runQuery(query, BRUTE_FORCE);
    runQuery(query, INDEXED);

    // MISS QUERY
    queryString = "select distinct * from " + SEPARATOR + "portfolios where type = 'miss'";
    query = qs.newQuery(queryString);
    runQuery(getMissHandQuery(queryString), HAND_CODED);
    runQuery(query, BRUTE_FORCE);
    runQuery(query, INDEXED);
  }

  private void runQuery(Query query, int execType) throws Exception {
    System.out.println("Executing Query: " + query.getQueryString());
    System.out.println("Num iterations=" + NUM_ITERATIONS);
    System.out.println();
    var indexed = execType == INDEXED;
    for (var num : DATA_SET_SIZES) {
      populate(num, indexed);
      System.out.println("Executing (" + execTypeStrings[execType] + ")...");
      var startTime = NanoTimer.getTime();
      SelectResults results = null;
      for (var j = 0; j < NUM_ITERATIONS; j++) {
        results = (SelectResults) query.execute();
      }
      var totalTime = NanoTimer.getTime() - startTime;
      System.out.println("results size =" + results.size());
      var aveTime = totalTime / NUM_ITERATIONS / 1e6f;
      System.out.println("ave execution time=" + aveTime + " ms");
      this.results[execType].add(aveTime);
      System.out.println();
    }
    System.out.println("--------------------------------------------");
  }

  // --------------------------------------------------------
  // Hand-Coded Queries

  private Query getType1HandQuery(String queryString) {
    return new HandQuery(queryString) {
      @Override
      public Object execute() {
        Region region = cache.getRegion(SEPARATOR + "portfolios");
        SelectResults results = new ResultsSet();
        for (final var o : region.values()) {
          var ptflo = (Portfolio) o;
          if ("type1".equals(ptflo.getType())) {
            results.add(ptflo);
          }
        }
        return results;
      }
    };
  }

  private Query getMissHandQuery(String queryString) {
    return new HandQuery(queryString) {
      @Override
      public Object execute() {
        Region region = cache.getRegion(SEPARATOR + "portfolios");
        SelectResults results = new ResultsSet();
        for (final var o : region.values()) {
          var ptflo = (Portfolio) o;
          if ("miss".equals(ptflo.getType())) {
            results.add(ptflo);
          }
        }
        return results;
      }
    };
  }


  // --------------------------------------------------------

  private void setUp() throws CacheException {
    ds = DistributedSystem.connect(new Properties());
    cache = CacheFactory.create(ds);
    var attributesFactory = new AttributesFactory();
    attributesFactory.setValueConstraint(Portfolio.class);
    regionAttributes = attributesFactory.create();
    qs = cache.getQueryService();
  }

  private void tearDown() {
    ds.disconnect();
  }

  private void populate(int numPortfolios, boolean indexed) throws CacheException, QueryException {
    System.out.println("Populating Cache with " + numPortfolios + " Portfolios");
    if (region != null) {
      region.localDestroyRegion();
    }
    region = cache.createRegion("portfolios", regionAttributes);
    for (var i = 0; i < numPortfolios; i++) {
      region.put(String.valueOf(i), new Portfolio(i));
    }
    if (indexed) {
      System.out.println("Creating index...");
      var startNanos = NanoTimer.getTime();
      qs.createIndex("portfolios", IndexType.FUNCTIONAL, "type", SEPARATOR + "portfolios");
      var createTime = (NanoTimer.getTime() - startNanos) / 1e6f;
      System.out.println("Index created in " + createTime + " ms.");
      results[INDEX_CREATE].add(createTime);
    }
  }

  private void warmUpIndexCreation() throws CacheException, QueryException {
    System.out.println("Populating Cache with 1000 Portfolios");
    if (region != null) {
      region.localDestroyRegion();
    }
    region = cache.createRegion("portfolios", regionAttributes);
    for (var i = 0; i < 1000; i++) {
      region.put(String.valueOf(i), new Portfolio(i));
    }
    System.out.println("Warming up index creation...");
    for (var i = 0; i < 20000; i++) {
      var index =
          qs.createIndex("portfolios", IndexType.FUNCTIONAL, "type", SEPARATOR + "portfolios");
      qs.removeIndex(index);
    }
  }

  /**
   * @param args the command line arguments
   */
  public static void main(String[] args) throws Exception {
    new PerfQuery().run();
  }


  abstract class HandQuery implements Query {
    private final String queryString;

    HandQuery(String queryString) {
      this.queryString = queryString;
    }

    @Override
    public abstract Object execute() throws FunctionDomainException, TypeMismatchException,
        NameResolutionException, QueryInvocationTargetException;


    @Override
    public void compile() throws TypeMismatchException, NameResolutionException {
      // already compiled
    }

    @Override
    public boolean isCompiled() {
      return true;
    }

    @Override
    public String getQueryString() {
      return queryString;
    }

    @Override
    public Object execute(Object[] params) throws FunctionDomainException, TypeMismatchException,
        NameResolutionException, QueryInvocationTargetException {
      throw new UnsupportedOperationException();
    }

    @Override
    public QueryStatistics getStatistics() {
      throw new UnsupportedOperationException();
    }

    public Set getRegionsInQuery() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object execute(RegionFunctionContext context) throws FunctionDomainException,
        TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object execute(RegionFunctionContext context, Object[] params)
        throws FunctionDomainException, TypeMismatchException, NameResolutionException,
        QueryInvocationTargetException {
      throw new UnsupportedOperationException();
    }

  }


}
