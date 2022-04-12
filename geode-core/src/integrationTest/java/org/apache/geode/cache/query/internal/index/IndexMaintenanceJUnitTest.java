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
package org.apache.geode.cache.query.internal.index;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.functional.StructSetOrResultsSet;
import org.apache.geode.cache.query.internal.DefaultQueryService;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.internal.index.AbstractIndex.RegionEntryToValuesMap;
import org.apache.geode.cache.query.internal.index.IndexStore.IndexStoreEntry;
import org.apache.geode.cache.query.internal.index.MemoryIndexStore.MemoryIndexStoreEntry;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.persistence.query.CloseableIterator;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.junit.categories.OQLIndexTest;

@Category({OQLIndexTest.class})
public class IndexMaintenanceJUnitTest {

  private QueryService qs;
  private Region region;
  private Set idSet;
  private boolean isInitDone = false;
  private IndexProtocol index;
  private boolean indexUsed = false;

  @Before
  public void setUp() throws Exception {
    idSet = new HashSet();

    CacheUtils.startCache();
    Cache cache = CacheUtils.getCache();
    region = CacheUtils.createRegion("portfolio", Portfolio.class);
    region.put("0", new Portfolio(0));
    region.put("1", new Portfolio(1));
    region.put("2", new Portfolio(2));
    region.put("3", new Portfolio(3));
    for (var j = 0; j < 6; ++j) {
      idSet.add(j + "");
    }
    qs = cache.getQueryService();
    index = (IndexProtocol) qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status",
        SEPARATOR + "portfolio");
    assertTrue(index instanceof CompactRangeIndex);
    isInitDone = true;
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
    DefaultQueryService.TEST_QUERY_HETEROGENEOUS_OBJECTS = false;
    IndexManager.TEST_RANGEINDEX_ONLY = false;
  }

  /**
   * Tests Index maintenance on heterogenous objects
   */
  @Test
  public void testIndexMaintenanceWithHeterogenousObjects() throws Exception {
    DefaultQueryService.TEST_QUERY_HETEROGENEOUS_OBJECTS = true;
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    Cache cache = CacheUtils.getCache();
    qs = cache.getQueryService();
    region = CacheUtils.createRegion("portfolio1", null);
    idSet.clear();
    var p = new Portfolio(4);
    region.put("4", p);
    idSet.add("" + p.getID());
    p = new Portfolio(5);
    region.put("5", p);
    idSet.add("" + p.getID());
    region.put("6", 6);
    var i1 =
        qs.createIndex("indx1", IndexType.FUNCTIONAL, "pf.getID()", SEPARATOR + "portfolio1 pf");
    var ri = (RangeIndex) i1;
    assertEquals(2, ri.valueToEntriesMap.size());
    var itr = ri.valueToEntriesMap.values().iterator();
    while (itr.hasNext()) {
      var re2ValMap = (RangeIndex.RegionEntryToValuesMap) itr.next();
      assertEquals(1, re2ValMap.map.size());
      var obj = re2ValMap.map.values().iterator().next();
      assertFalse(obj instanceof Collection);
      assertTrue(obj instanceof Portfolio);
      var pf = (Portfolio) obj;
      assertTrue(idSet.contains(String.valueOf(pf.getID())));
    }
    assertEquals(1, ri.undefinedMappedEntries.map.size());
    var entry = (Map.Entry) ri.undefinedMappedEntries.map.entrySet().iterator().next();
    assertFalse(entry.getValue() instanceof Collection);
    assertTrue(entry.getValue() instanceof Integer);
    assertTrue(entry.getValue().equals(6));

    region.put("7", 7);
    idSet.add(7);
    assertEquals(2, ri.undefinedMappedEntries.map.size());
    itr = ri.undefinedMappedEntries.map.entrySet().iterator();
    while (itr.hasNext()) {
      entry = (Map.Entry) itr.next();
      assertFalse(entry.getValue() instanceof Collection);
      assertTrue(entry.getValue() instanceof Integer);
      idSet.contains(entry.getValue());
    }

    region.remove("7");
    idSet.remove(7);
    var i2 =
        qs.createIndex("indx2", IndexType.FUNCTIONAL, "pf.pkid", SEPARATOR + "portfolio1 pf");
    ri = (RangeIndex) i2;
    assertEquals(2, ri.valueToEntriesMap.size());
    itr = ri.valueToEntriesMap.values().iterator();
    while (itr.hasNext()) {
      var re2ValMap = (RangeIndex.RegionEntryToValuesMap) itr.next();
      assertEquals(1, re2ValMap.map.size());
      var obj = re2ValMap.map.values().iterator().next();
      assertFalse(obj instanceof Collection);
      assertTrue(obj instanceof Portfolio);
      var pf = (Portfolio) obj;
      assertTrue(idSet.contains(String.valueOf(pf.getID())));
    }
    assertEquals(1, ri.undefinedMappedEntries.map.size());
    entry = (Map.Entry) ri.undefinedMappedEntries.map.entrySet().iterator().next();
    assertFalse(entry.getValue() instanceof Collection);
    assertTrue(entry.getValue() instanceof Integer);
    assertTrue(entry.getValue().equals(6));

    region.put("7", 7);
    idSet.add(7);
    assertEquals(2, ri.undefinedMappedEntries.map.size());
    itr = ri.undefinedMappedEntries.map.entrySet().iterator();
    while (itr.hasNext()) {
      entry = (Map.Entry) itr.next();
      assertFalse(entry.getValue() instanceof Collection);
      assertTrue(entry.getValue() instanceof Integer);
      idSet.contains(entry.getValue());
    }
  }

  /**
   * Tests query on region containing heterogenous objects
   */
  @Test
  public void testQueryOnHeterogenousObjects() throws Exception {
    DefaultQueryService.TEST_QUERY_HETEROGENEOUS_OBJECTS = true;
    Cache cache = CacheUtils.getCache();
    region = CacheUtils.createRegion("portfolio1", null);
    for (var i = 0; i < 5; ++i) {
      var p = new Portfolio(i + 1);
      region.put(i + 1, p);
    }

    for (var i = 5; i < 10; ++i) {
      region.put(i + 1, i + 1);
    }
    var queryStr =
        "Select distinct * from " + SEPARATOR + "portfolio1 pf1 where pf1.getID() > 3";
    var q = qs.newQuery(queryStr);
    var rs = (SelectResults) q.execute();
    assertEquals(2, rs.size());
    var itr = rs.iterator();
    while (itr.hasNext()) {
      var p = (Portfolio) itr.next();
      assertTrue(p == region.get(4) || p == region.get(5));
    }

    var i1 =
        qs.createIndex("indx1", IndexType.FUNCTIONAL, "pf.getID()", SEPARATOR + "portfolio1 pf");
    var old = QueryObserverHolder.setInstance(new QueryObserverAdapter() {
      private boolean indexUsed = false;

      @Override
      public void beforeIndexLookup(Index index, int oper, Object key) {
        indexUsed = true;
      }

      @Override
      public void endQuery() {
        assertTrue(indexUsed);
      }
    });

    rs = (SelectResults) q.execute();
    assertEquals(2, rs.size());
    itr = rs.iterator();
    while (itr.hasNext()) {
      var p = (Portfolio) itr.next();
      assertTrue(p == region.get(4) || p == region.get(5));
    }
    qs.removeIndex(i1);

    queryStr = "Select distinct * from " + SEPARATOR + "portfolio1 pf1 where pf1.pkid > '3'";
    q = qs.newQuery(queryStr);
    rs = (SelectResults) q.execute();
    assertEquals(2, rs.size());
    itr = rs.iterator();
    while (itr.hasNext()) {
      var p = (Portfolio) itr.next();
      assertTrue(p == region.get(4) || p == region.get(5));
    }

    i1 = qs.createIndex("indx1", IndexType.FUNCTIONAL, "pf.pkid", SEPARATOR + "portfolio1 pf");
    QueryObserverHolder.setInstance(new QueryObserverAdapter() {
      private boolean indexUsed = false;

      @Override
      public void beforeIndexLookup(Index index, int oper, Object key) {
        indexUsed = true;
      }

      @Override
      public void endQuery() {
        assertTrue(indexUsed);
      }
    });

    rs = (SelectResults) q.execute();
    assertEquals(2, rs.size());
    itr = rs.iterator();
    while (itr.hasNext()) {
      var p = (Portfolio) itr.next();
      assertTrue(p == region.get(4) || p == region.get(5));
    }
  }

  /**
   * Tests Index maintenance on method Keys() as iterator ( with focus on behaviour if not
   * implemented in DummyQRegion
   */
  @Test
  public void testIndexMaintenanceWithIndexOnMethodKeys() throws Exception {
    var i1 = qs.createIndex("indx1", IndexType.FUNCTIONAL, "ks.toString",
        SEPARATOR + "portfolio.keys() ks");
    CacheUtils.getCache();
    region = CacheUtils.getRegion(SEPARATOR + "portfolio");
    region.put("4", new Portfolio(4));
    region.put("5", new Portfolio(5));
    var ri = (CompactRangeIndex) i1;
    validateIndexForKeys(ri);
  }

  /**
   * Tests Index maintenance on method asList() as iterator ( with focus on behaviour if not
   * implemented in DummyQRegion
   */
  @Test
  public void testIndexMaintenanceWithIndexOnMethodAsList() throws Exception {
    var i1 = qs.createIndex("indx1", IndexType.FUNCTIONAL, "pf.getID",
        SEPARATOR + "portfolio.asList() pf");
    CacheUtils.getCache();
    region = CacheUtils.getRegion(SEPARATOR + "portfolio");
    region.put("4", new Portfolio(4));
    region.put("5", new Portfolio(5));
    var ri = (CompactRangeIndex) i1;
    validateIndexForValues(ri);
  }

  /**
   * Tests Index maintenance on method values() as iterator ( with focus on behaviour if not
   * implemented in DummyQRegion
   */
  @Test
  public void testIndexMaintenanceWithIndexOnMethodValues() throws Exception {
    var i1 = qs.createIndex("indx1", IndexType.FUNCTIONAL, "pf.getID",
        SEPARATOR + "portfolio.values() pf");
    assertTrue(i1 instanceof CompactRangeIndex);
    Cache cache = CacheUtils.getCache();
    region = CacheUtils.getRegion(SEPARATOR + "portfolio");
    region.put("4", new Portfolio(4));
    region.put("5", new Portfolio(5));
    var ri = (CompactRangeIndex) i1;
    validateIndexForValues(ri);
  }

  /**
   * Tests Index maintenance on method getValues() as iterator ( with focus on behaviour if not
   * implemented in DummyQRegion
   */
  @Test
  public void testIndexMaintenanceWithIndexOnMethodGetValues() throws Exception {
    var i1 =
        qs.createIndex("indx1", IndexType.FUNCTIONAL, "pf.getID",
            SEPARATOR + "portfolio.getValues() pf");
    assertTrue(i1 instanceof CompactRangeIndex);
    Cache cache = CacheUtils.getCache();
    region = CacheUtils.getRegion(SEPARATOR + "portfolio");
    region.put("4", new Portfolio(4));
    region.put("5", new Portfolio(5));
    var ri = (CompactRangeIndex) i1;
    validateIndexForValues(ri);
  }

  /**
   * Tests Index maintenance on method toArray() as iterator ( with focus on behaviour if not
   * implemented in DummyQRegion
   */
  @Test
  public void testIndexMaintenanceWithIndexOnMethodtoArray() throws Exception {
    var i1 = qs.createIndex("indx1", IndexType.FUNCTIONAL, "pf.getID",
        SEPARATOR + "portfolio.toArray() pf");
    assertTrue(i1 instanceof CompactRangeIndex);
    Cache cache = CacheUtils.getCache();
    region = CacheUtils.getRegion(SEPARATOR + "portfolio");
    region.put("4", new Portfolio(4));
    region.put("5", new Portfolio(5));
    var ri = (CompactRangeIndex) i1;
    validateIndexForValues(ri);
  }

  /**
   * Tests Index maintenance on method asSet() as iterator ( with focus on behaviour if not
   * implemented in DummyQRegion
   */
  @Test
  public void testIndexMaintenanceWithIndexOnMethodAsSet() throws Exception {
    var i1 = qs.createIndex("indx1", IndexType.FUNCTIONAL, "pf.getID",
        SEPARATOR + "portfolio.asSet() pf");
    CacheUtils.getCache();
    region = CacheUtils.getRegion(SEPARATOR + "portfolio");
    region.put("4", new Portfolio(4));
    region.put("5", new Portfolio(5));
    var ri = (CompactRangeIndex) i1;
    validateIndexForValues(ri);
  }

  /**
   * Tests Index maintenance on method keySet() as iterator ( with focus on behaviour if not
   * implemented in DummyQRegion
   */
  @Test
  public void testIndexMaintenanceWithIndexOnMethodKeySet() throws Exception {
    var i1 =
        qs.createIndex("indx1", IndexType.FUNCTIONAL, "ks.toString",
            SEPARATOR + "portfolio.keySet() ks");
    CacheUtils.getCache();
    region = CacheUtils.getRegion(SEPARATOR + "portfolio");
    region.put("4", new Portfolio(4));
    region.put("5", new Portfolio(5));
    var ri = (CompactRangeIndex) i1;
    validateIndexForKeys(ri);
  }

  /**
   * Tests Index maintenance on method getKeys() as iterator ( with focus on behaviour if not
   * implemented in DummyQRegion
   */
  @Test
  public void testIndexMaintenanceWithIndexOnMethodGetKeys() throws Exception {
    var i1 =
        qs.createIndex("indx1", IndexType.FUNCTIONAL, "ks.toString",
            SEPARATOR + "portfolio.getKeys() ks");
    CacheUtils.getCache();
    region = CacheUtils.getRegion(SEPARATOR + "portfolio");
    region.put("4", new Portfolio(4));
    region.put("5", new Portfolio(5));
    var ri = (CompactRangeIndex) i1;
    validateIndexForKeys(ri);
  }

  /**
   * Tests Index maintenance on method entrySet() as iterator ( with focus on behaviour if not
   * implemented in DummyQRegion
   */
  @Test
  public void testIndexMaintenanceWithIndexOnMethodEntrySet() throws Exception {
    var i1 = qs.createIndex("indx1", IndexType.FUNCTIONAL, "entries.value.getID",
        SEPARATOR + "portfolio.entrySet() entries");
    CacheUtils.getCache();
    region = CacheUtils.getRegion(SEPARATOR + "portfolio");
    region.put("4", new Portfolio(4));
    region.put("5", new Portfolio(5));
    var ri = (CompactRangeIndex) i1;
    validateIndexForEntries(ri);
  }

  /**
   * Tests Index maintenance on method getEntries( ) as iterator ( with focus on behaviour if not
   * implemented in DummyQRegion
   */
  @Test
  public void testIndexMaintenanceWithIndexOnMethodGetEntries() throws Exception {
    var i1 = qs.createIndex("indx1", IndexType.FUNCTIONAL, "entries.value.getID",
        SEPARATOR + "portfolio.getEntries() entries");
    CacheUtils.getCache();
    region = CacheUtils.getRegion(SEPARATOR + "portfolio");
    region.put("4", new Portfolio(4));
    region.put("5", new Portfolio(5));
    var ri = (CompactRangeIndex) i1;
    validateIndexForEntries(ri);
  }

  @Test
  public void testMapKeyIndexMaintenanceForNonCompactTypeAllKeysIndex() throws Exception {
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    QueryService qs;
    qs = CacheUtils.getQueryService();
    var testRgn = (LocalRegion) CacheUtils.createRegion("testRgn", null);
    var ID = 1;
    // Add some test data now
    // Add 5 main objects. 1 will contain key1, 2 will contain key1 & key2
    // and so on
    for (; ID <= 5; ++ID) {
      var mkid = new MapKeyIndexData(ID);
      for (var j = 1; j <= ID; ++j) {
        mkid.maap.put("key" + j, "val" + j);
      }
      testRgn.put(ID, mkid);
    }
    --ID;
    var i1 =
        qs.createIndex("Index1", IndexType.FUNCTIONAL, "objs.maap[*]", SEPARATOR + "testRgn objs");
    assertEquals(i1.getCanonicalizedIndexedExpression(), "index_iter1.maap[*]");
    assertTrue(i1 instanceof MapRangeIndex);
    var mri = (MapRangeIndex) i1;
    // Test index maintenance
    // addition of new Portfolio object
    var indxMap = mri.getRangeIndexHolderForTesting();
    assertEquals(indxMap.size(), ID);
    for (var j = 1; j <= ID; ++j) {
      assertTrue(indxMap.containsKey("key" + j));
      var rng = (RangeIndex) indxMap.get("key" + j);
      var itr = rng.valueToEntriesMap.values().iterator();
      assertEquals(rng.valueToEntriesMap.size(), 1);
      assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      Set<Integer> expectedElements = new HashSet<>();
      for (var k = j; k <= ID; ++k) {
        expectedElements.add(k);
      }
      while (itr.hasNext()) {
        var entryMap = (RegionEntryToValuesMap) itr.next();
        assertEquals(ID + 1 - j, entryMap.getNumEntries());
        for (var elem : expectedElements) {
          var re = testRgn.basicGetEntry(elem);

          assertTrue(entryMap.containsEntry(re));
        }
      }
    }
    // addition of new Portfolio in the Map
    ++ID;// ID = 6;
    var mkid = new MapKeyIndexData(ID);
    for (var j = 1; j <= ID; ++j) {
      mkid.maap.put("key" + j, "val" + j);
    }
    testRgn.put(ID, mkid);
    assertEquals(indxMap.size(), ID);
    for (var j = 1; j <= ID; ++j) {
      assertTrue(indxMap.containsKey("key" + j));
      var rng = (RangeIndex) indxMap.get("key" + j);
      var itr = rng.valueToEntriesMap.values().iterator();
      assertEquals(rng.valueToEntriesMap.size(), 1);
      assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      Set<Integer> expectedElements = new HashSet<>();
      for (var k = j; k <= ID; ++k) {
        expectedElements.add(k);
      }
      while (itr.hasNext()) {
        var entryMap = (RegionEntryToValuesMap) itr.next();
        assertEquals(ID + 1 - j, entryMap.getNumEntries());
        for (var elem : expectedElements) {
          var re = testRgn.basicGetEntry(elem);

          assertTrue(entryMap.containsEntry(re));
        }
      }
    }
    // addition of new key in the positions map
    mkid.maap.put("key7", "val7");
    testRgn.put(ID, mkid);
    assertEquals(indxMap.size(), 7);

    for (var j = 1; j <= ID; ++j) {
      assertTrue(indxMap.containsKey("key" + j));
      var rng = (RangeIndex) indxMap.get("key" + j);
      var itr = rng.valueToEntriesMap.values().iterator();
      assertEquals(rng.valueToEntriesMap.size(), 1);
      assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      Set<Integer> expectedElements = new HashSet<>();
      for (var k = j; k <= ID; ++k) {
        expectedElements.add(k);
      }
      while (itr.hasNext()) {
        var entryMap = (RegionEntryToValuesMap) itr.next();
        assertEquals(ID + 1 - j, entryMap.getNumEntries());
        for (var elem : expectedElements) {
          var re = testRgn.basicGetEntry(elem);
          assertTrue(entryMap.containsEntry(re));
        }
      }
    }
    assertTrue(indxMap.containsKey("key7"));
    var rng = (RangeIndex) indxMap.get("key7");
    var itr = rng.valueToEntriesMap.values().iterator();
    assertEquals(rng.valueToEntriesMap.size(), 1);
    assertTrue(rng.valueToEntriesMap.containsKey("val7"));
    var entryMap = (RegionEntryToValuesMap) itr.next();
    assertEquals(1, entryMap.getNumEntries());
    var re = testRgn.basicGetEntry(6);
    entryMap.containsEntry(re);
    // deletion of key in the positions map
    mkid.maap.remove("key7");
    testRgn.put(ID, mkid);
    assertEquals(indxMap.size(), ID + 1);
    for (var j = 1; j <= ID; ++j) {
      assertTrue(indxMap.containsKey("key" + j));
      rng = (RangeIndex) indxMap.get("key" + j);
      itr = rng.valueToEntriesMap.values().iterator();
      assertEquals(rng.valueToEntriesMap.size(), 1);
      assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      Set<Integer> expectedElements = new HashSet<>();
      for (var k = j; k <= ID; ++k) {
        expectedElements.add(k);
      }
      while (itr.hasNext()) {
        entryMap = (RegionEntryToValuesMap) itr.next();
        assertEquals(ID + 1 - j, entryMap.getNumEntries());
        for (var elem : expectedElements) {
          re = testRgn.basicGetEntry(elem);
          assertTrue(entryMap.containsEntry(re));
        }
      }
    }
    // update of key in the positions map
    mkid = (MapKeyIndexData) testRgn.get(1);
    mkid.maap.put("key1", "val2");
    testRgn.put(1, mkid);
    assertEquals(indxMap.size(), ID + 1);
    for (var j = 1; j <= ID; ++j) {
      var keey = "key" + j;
      assertTrue(indxMap.containsKey(keey));
      rng = (RangeIndex) indxMap.get(keey);
      itr = rng.valueToEntriesMap.values().iterator();
      if (keey.equals("key1")) {
        assertEquals(rng.valueToEntriesMap.size(), 2);
        assertTrue(rng.valueToEntriesMap.containsKey("val1"));
        assertTrue(rng.valueToEntriesMap.containsKey("val2"));
      } else {
        assertEquals(rng.valueToEntriesMap.size(), 1);
        assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      }

      Set<Integer> expectedElements = new HashSet<>();
      for (var k = j; k <= ID; ++k) {
        expectedElements.add(k);
      }
      if (keey.equals("key1")) {
        assertEquals(rng.valueToEntriesMap.size(), 2);
        entryMap = (RegionEntryToValuesMap) rng.valueToEntriesMap.get("val1");
        assertEquals(5, entryMap.getNumEntries());
        expectedElements.remove(1);
        for (var elem : expectedElements) {
          re = testRgn.basicGetEntry(elem);

          assertTrue(entryMap.containsEntry(re));
        }
        entryMap = (RegionEntryToValuesMap) rng.valueToEntriesMap.get("val2");
        assertEquals(1, entryMap.getNumEntries());
        re = testRgn.basicGetEntry(1);
        assertTrue(entryMap.containsEntry(re));

      } else {
        while (itr.hasNext()) {
          entryMap = (RegionEntryToValuesMap) itr.next();
          assertEquals(ID + 1 - j, entryMap.getNumEntries());
          for (var elem : expectedElements) {
            re = testRgn.basicGetEntry(elem);

            assertTrue(entryMap.containsEntry(re));
          }
        }
      }
    }
    // deletion of portfolio object key in the positions map
    testRgn.remove(ID);
    --ID;// ID = 5;
    // No Key Indexes are removed from a MapRangeIndex even if they are empty.
    assertEquals(indxMap.size(), ID + 2);
    for (var j = 1; j <= ID; ++j) {
      var keey = "key" + j;
      assertTrue(indxMap.containsKey(keey));
      rng = (RangeIndex) indxMap.get(keey);
      itr = rng.valueToEntriesMap.values().iterator();
      if (keey.equals("key1")) {
        assertEquals(rng.valueToEntriesMap.size(), 2);
        assertTrue(rng.valueToEntriesMap.containsKey("val1"));
        assertTrue(rng.valueToEntriesMap.containsKey("val2"));
      } else {
        assertEquals(rng.valueToEntriesMap.size(), 1);
        assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      }

      Set<Integer> expectedElements = new HashSet<>();
      for (var k = j; k <= ID; ++k) {
        expectedElements.add(k);
      }
      if (keey.equals("key1")) {
        assertEquals(rng.valueToEntriesMap.size(), 2);
        entryMap = (RegionEntryToValuesMap) rng.valueToEntriesMap.get("val1");
        assertEquals(4, entryMap.getNumEntries());
        expectedElements.remove(1);
        for (var elem : expectedElements) {
          re = testRgn.basicGetEntry(elem);

          assertTrue(entryMap.containsEntry(re));
        }
        entryMap = (RegionEntryToValuesMap) rng.valueToEntriesMap.get("val2");
        assertEquals(1, entryMap.getNumEntries());
        re = testRgn.basicGetEntry(1);
        assertTrue(entryMap.containsEntry(re));

      } else {
        while (itr.hasNext()) {
          entryMap = (RegionEntryToValuesMap) itr.next();
          assertEquals(ID + 1 - j, entryMap.getNumEntries());
          for (var elem : expectedElements) {
            re = testRgn.basicGetEntry(elem);

            assertTrue(entryMap.containsEntry(re));
          }
        }
      }
    }
  }

  @Test
  public void testMapKeyIndexMaintenanceForNonCompactTypeSpecificKeysIndex() throws Exception {
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    QueryService qs;
    qs = CacheUtils.getQueryService();
    var testRgn = (LocalRegion) CacheUtils.createRegion("testRgn", null);
    var ID = 1;
    // Add some test data now
    // Add 5 main objects. 1 will contain key1, 2 will contain key1 & key2
    // and so on
    for (; ID <= 5; ++ID) {
      var mkid = new MapKeyIndexData(ID);
      for (var j = 1; j <= ID; ++j) {
        mkid.maap.put("key" + j, "val" + j);
      }
      testRgn.put(ID, mkid);
    }
    --ID;// ID = 5;
    var i1 = qs.createIndex("Index1", IndexType.FUNCTIONAL,
        "objs.maap['key1','key2','key3','key7']", SEPARATOR + "testRgn objs");
    assertEquals(i1.getCanonicalizedIndexedExpression(),
        "index_iter1.maap['key1','key2','key3','key7']");
    assertTrue(i1 instanceof MapRangeIndex);
    var mri = (MapRangeIndex) i1;
    // Test index maintenance
    // addition of new Portfolio object
    var indxMap = mri.getRangeIndexHolderForTesting();
    assertEquals(indxMap.size(), 4);
    for (var j = 1; j <= 3; ++j) {
      assertTrue(indxMap.containsKey("key" + j));
      var rng = (RangeIndex) indxMap.get("key" + j);
      var itr = rng.valueToEntriesMap.values().iterator();
      assertEquals(rng.valueToEntriesMap.size(), 1);
      assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      Set<Integer> expectedElements = new HashSet<>();
      for (var k = j; k <= ID; ++k) {
        expectedElements.add(k);
      }
      while (itr.hasNext()) {
        var entryMap = (RegionEntryToValuesMap) itr.next();
        assertEquals(ID + 1 - j, entryMap.getNumEntries());
        for (var elem : expectedElements) {
          var re = testRgn.basicGetEntry(elem);

          assertTrue(entryMap.containsEntry(re));
        }
      }
    }
    for (var j = 4; j <= ID; ++j) {
      assertFalse(indxMap.containsKey("key" + j));
    }
    // addition of new Portfolio in the Map
    ++ID; // ID = 6
    var mkid = new MapKeyIndexData(ID);
    for (var j = 1; j <= ID; ++j) {
      mkid.maap.put("key" + j, "val" + j);
    }
    testRgn.put(ID, mkid);
    assertEquals(indxMap.size(), 4);
    for (var j = 1; j <= 3; ++j) {
      assertTrue(indxMap.containsKey("key" + j));
      var rng = (RangeIndex) indxMap.get("key" + j);
      var itr = rng.valueToEntriesMap.values().iterator();
      assertEquals(rng.valueToEntriesMap.size(), 1);
      assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      Set<Integer> expectedElements = new HashSet<>();
      for (var k = j; k <= ID; ++k) {
        expectedElements.add(k);
      }
      while (itr.hasNext()) {
        var entryMap = (RegionEntryToValuesMap) itr.next();
        assertEquals(ID + 1 - j, entryMap.getNumEntries());
        for (var elem : expectedElements) {
          var re = testRgn.basicGetEntry(elem);
          assertTrue(entryMap.containsEntry(re));
        }
      }
    }
    for (var j = 4; j <= ID; ++j) {
      assertFalse(indxMap.containsKey("key" + j));
    }
    // addition of new key in the positions map
    mkid.maap.put("key7", "val7");
    testRgn.put(ID, mkid);
    assertEquals(indxMap.size(), 4);

    for (var j = 1; j <= 3; ++j) {
      assertTrue(indxMap.containsKey("key" + j));
      var rng = (RangeIndex) indxMap.get("key" + j);
      var itr = rng.valueToEntriesMap.values().iterator();
      assertEquals(rng.valueToEntriesMap.size(), 1);
      assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      Set<Integer> expectedElements = new HashSet<>();
      for (var k = j; k <= ID; ++k) {
        expectedElements.add(k);
      }
      while (itr.hasNext()) {
        var entryMap = (RegionEntryToValuesMap) itr.next();
        assertEquals(ID + 1 - j, entryMap.getNumEntries());
        for (var elem : expectedElements) {
          var re = testRgn.basicGetEntry(elem);

          assertTrue(entryMap.containsEntry(re));
        }
      }
    }
    assertTrue(indxMap.containsKey("key7"));
    var rng = (RangeIndex) indxMap.get("key7");
    var itr = rng.valueToEntriesMap.values().iterator();
    assertEquals(rng.valueToEntriesMap.size(), 1);
    assertTrue(rng.valueToEntriesMap.containsKey("val7"));
    var entryMap = (RegionEntryToValuesMap) itr.next();
    assertEquals(1, entryMap.getNumEntries());
    var re = testRgn.basicGetEntry(6);
    entryMap.containsEntry(re);

    // deletion of key in the positions map
    mkid.maap.remove("key7");
    testRgn.put(ID, mkid);
    // No Key Indexes are removed from a MapRangeIndex even if they are empty.
    assertEquals(indxMap.size(), 4);
    for (var j = 1; j <= 3; ++j) {
      assertTrue(indxMap.containsKey("key" + j));
      rng = (RangeIndex) indxMap.get("key" + j);
      itr = rng.valueToEntriesMap.values().iterator();
      assertEquals(rng.valueToEntriesMap.size(), 1);
      assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      Set<Integer> expectedElements = new HashSet<>();
      for (var k = j; k <= ID; ++k) {
        expectedElements.add(k);
      }
      while (itr.hasNext()) {
        entryMap = (RegionEntryToValuesMap) itr.next();
        assertEquals(ID + 1 - j, entryMap.getNumEntries());
        for (var elem : expectedElements) {
          re = testRgn.basicGetEntry(elem);

          assertTrue(entryMap.containsEntry(re));
        }
      }
    }
    // update of key in the positions map
    mkid = (MapKeyIndexData) testRgn.get(1);
    mkid.maap.put("key1", "val2");
    testRgn.put(1, mkid);
    assertEquals(indxMap.size(), 4);
    for (var j = 1; j <= 3; ++j) {
      var keey = "key" + j;
      assertTrue(indxMap.containsKey(keey));
      rng = (RangeIndex) indxMap.get(keey);
      itr = rng.valueToEntriesMap.values().iterator();
      if (keey.equals("key1")) {
        assertEquals(rng.valueToEntriesMap.size(), 2);
        assertTrue(rng.valueToEntriesMap.containsKey("val1"));
        assertTrue(rng.valueToEntriesMap.containsKey("val2"));
      } else {
        assertEquals(rng.valueToEntriesMap.size(), 1);
        assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      }

      Set<Integer> expectedElements = new HashSet<>();
      for (var k = j; k <= ID; ++k) {
        expectedElements.add(k);
      }
      if (keey.equals("key1")) {
        assertEquals(rng.valueToEntriesMap.size(), 2);
        entryMap = (RegionEntryToValuesMap) rng.valueToEntriesMap.get("val1");
        assertEquals(5, entryMap.getNumEntries());
        expectedElements.remove(1);
        for (var elem : expectedElements) {
          re = testRgn.basicGetEntry(elem);
          assertTrue(entryMap.containsEntry(re));
        }
        entryMap = (RegionEntryToValuesMap) rng.valueToEntriesMap.get("val2");
        assertEquals(1, entryMap.getNumEntries());
        re = testRgn.basicGetEntry(1);
        assertTrue(entryMap.containsEntry(re));

      } else {
        while (itr.hasNext()) {
          entryMap = (RegionEntryToValuesMap) itr.next();
          assertEquals(ID + 1 - j, entryMap.getNumEntries());
          for (var elem : expectedElements) {
            re = testRgn.basicGetEntry(elem);
            assertTrue(entryMap.containsEntry(re));
          }
        }
      }
    }
    // deletion of portfolio object key in the positions map
    testRgn.remove(3);

    assertEquals(indxMap.size(), 4);
    for (var j = 1; j <= 3; ++j) {
      var keey = "key" + j;
      assertTrue(indxMap.containsKey(keey));
      rng = (RangeIndex) indxMap.get(keey);
      itr = rng.valueToEntriesMap.values().iterator();
      if (keey.equals("key1")) {
        assertEquals(rng.valueToEntriesMap.size(), 2);
        assertTrue(rng.valueToEntriesMap.containsKey("val1"));
        assertTrue(rng.valueToEntriesMap.containsKey("val2"));
      } else {
        assertEquals(rng.valueToEntriesMap.size(), 1);
        assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      }
      if (keey.equals("key1")) {
        assertEquals(rng.valueToEntriesMap.size(), 2);
        entryMap = (RegionEntryToValuesMap) rng.valueToEntriesMap.get("val1");
        assertEquals(4, entryMap.getNumEntries());

        for (var k = 2; k <= 6; ++k) {
          if (k == 3) {
            continue;
          } else {
            re = testRgn.basicGetEntry(k);
            assertTrue(entryMap.containsEntry(re));
          }
        }
        entryMap = (RegionEntryToValuesMap) rng.valueToEntriesMap.get("val2");
        assertEquals(1, entryMap.getNumEntries());
        re = testRgn.basicGetEntry(1);
        assertTrue(entryMap.containsEntry(re));

      } else {
        while (itr.hasNext()) {
          entryMap = (RegionEntryToValuesMap) itr.next();
          assertEquals(ID - j, entryMap.getNumEntries());
          for (var p = j; p <= ID; ++p) {
            re = testRgn.basicGetEntry(p);
            if (p == 3) {
              assertNull(re);
            } else {
              assertTrue(entryMap.containsEntry(re));
            }
          }
        }
      }
    }
  }

  @Test
  public void testMapIndexRecreationForAllKeys() throws Exception {
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    QueryService qs;
    qs = CacheUtils.getQueryService();
    var testRgn = (LocalRegion) CacheUtils.createRegion("testRgn", null);
    var ID = 1;
    // Add some test data now
    // Add 5 main objects. 1 will contain key1, 2 will contain key1 & key2
    // and so on
    for (; ID <= 5; ++ID) {
      var mkid = new MapKeyIndexData(ID);
      for (var j = 1; j <= ID; ++j) {
        mkid.maap.put("key" + j, "val" + j);
      }
      testRgn.put(ID, mkid);
    }
    --ID;
    var i1 =
        qs.createIndex("Index1", IndexType.FUNCTIONAL, "objs.maap[*]", SEPARATOR + "testRgn objs");
    assertEquals(i1.getCanonicalizedIndexedExpression(), "index_iter1.maap[*]");
    assertTrue(i1 instanceof MapRangeIndex);
    var mri = (MapRangeIndex) i1;
    // Test index maintenance
    // addition of new Portfolio object
    var indxMap = mri.getRangeIndexHolderForTesting();
    assertEquals(indxMap.size(), ID);
    for (var j = 1; j <= ID; ++j) {
      assertTrue(indxMap.containsKey("key" + j));
      var rng = (RangeIndex) indxMap.get("key" + j);
      var itr = rng.valueToEntriesMap.values().iterator();
      assertEquals(rng.valueToEntriesMap.size(), 1);
      assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      Set<Integer> expectedElements = new HashSet<>();
      for (var k = j; k <= ID; ++k) {
        expectedElements.add(k);
      }
      while (itr.hasNext()) {
        var entryMap = (RegionEntryToValuesMap) itr.next();
        assertEquals(ID + 1 - j, entryMap.getNumEntries());
        for (var elem : expectedElements) {
          var re = testRgn.basicGetEntry(elem);
          assertTrue(entryMap.containsEntry(re));
        }
      }
    }
    var im = testRgn.getIndexManager();
    im.rerunIndexCreationQuery();
    ID = 5;
    i1 = im.getIndex("Index1");
    assertEquals(i1.getCanonicalizedIndexedExpression(), "index_iter1.maap[*]");
    assertTrue(i1 instanceof MapRangeIndex);
    mri = (MapRangeIndex) i1;
    // Test index maintenance
    // addition of new Portfolio object
    indxMap = mri.getRangeIndexHolderForTesting();
    assertEquals(indxMap.size(), ID);
    for (var j = 1; j <= ID; ++j) {
      assertTrue(indxMap.containsKey("key" + j));
      var rng = (RangeIndex) indxMap.get("key" + j);
      var itr = rng.valueToEntriesMap.values().iterator();
      assertEquals(rng.valueToEntriesMap.size(), 1);
      assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      Set<Integer> expectedElements = new HashSet<>();
      for (var k = j; k <= ID; ++k) {
        expectedElements.add(k);
      }
      while (itr.hasNext()) {
        var entryMap = (RegionEntryToValuesMap) itr.next();
        assertEquals(ID + 1 - j, entryMap.getNumEntries());
        for (var elem : expectedElements) {
          var re = testRgn.basicGetEntry(elem);
          assertTrue(entryMap.containsEntry(re));
        }
      }
    }
  }

  /**
   * Tests Index maintenance on data loaded via cache loader
   */
  @Test
  public void testIndexMaintenanceOnCacheLoadedData() throws Exception {
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    Cache cache = CacheUtils.getCache();
    qs = cache.getQueryService();
    region = CacheUtils.createRegion("portfolio1", null);
    var am = region.getAttributesMutator();
    am.setCacheLoader(new CacheLoader() {

      @Override
      public Object load(LoaderHelper helper) throws CacheLoaderException {
        var key = (String) helper.getKey();
        var p = new Portfolio(Integer.parseInt(key));
        return p;
      }

      @Override
      public void close() {
        // nothing
      }
    });

    var i1 =
        qs.createIndex("indx1", IndexType.FUNCTIONAL, "pf.getID()", SEPARATOR + "portfolio1 pf");
    List keys = new ArrayList();
    keys.add("1");
    keys.add("2");
    keys.add("3");
    keys.add("4");

    region.getAll(keys);
  }

  /**
   * Tests Index maintenance on data loaded via cache loader
   */
  @Test
  public void testIndexMaintenanceOnPutAll() throws Exception {
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    Cache cache = CacheUtils.getCache();
    qs = cache.getQueryService();
    region = CacheUtils.createRegion("portfolio1", null);
    region.put("1", new Portfolio(1));
    var i1 = qs.createIndex("indx1", IndexType.FUNCTIONAL, "posvals.secId",
        SEPARATOR + "portfolio1 pf, pf.positions.values posvals ");
    Map data = new HashMap();
    for (var i = 1; i < 11; ++i) {
      data.put("" + i, new Portfolio(i + 2));
    }

    region.putAll(data);
  }

  @Test
  public void testBug43597() throws Exception {
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    Cache cache = CacheUtils.getCache();
    qs = cache.getQueryService();
    region = CacheUtils.createRegion("portfolio1", null);
    var i1 = qs.createIndex("indx1", IndexType.FUNCTIONAL, "posvals",
        SEPARATOR + "portfolio1 pf, pf.getCollectionHolderMap.values posvals ");
    var pf1 = new Portfolio(1);
    Map collHolderMap = pf1.getCollectionHolderMap();
    collHolderMap.clear();
    collHolderMap.put(1, 1);
    collHolderMap.put(2, 1);
    region.put("1", pf1);

    pf1 = new Portfolio(2);
    collHolderMap = pf1.getCollectionHolderMap();
    collHolderMap.clear();
    collHolderMap.put(3, 1);
    collHolderMap.put(4, 1);
    region.put("1", pf1);
  }

  /**
   * Starting of section - moved from wrongly spelled old class "IndexMaintainceJUnitTest".
   * GEODE-1505
   */

  @Test
  public void fromClausesAndIndexedExpressionsMatchExpectedValues()
      throws IndexNameConflictException, IndexExistsException, RegionNotFoundException {
    var i1 = qs.createIndex("tIndex", IndexType.FUNCTIONAL, "vals.secId",
        SEPARATOR + "portfolio pf, pf.positions.values vals");
    var i2 = qs.createIndex("dIndex", IndexType.FUNCTIONAL, "pf.getCW(pf.ID)",
        SEPARATOR + "portfolio pf");
    var i3 = qs.createIndex("fIndex", IndexType.FUNCTIONAL, "sIter",
        SEPARATOR + "portfolio pf, pf.collectionHolderMap[(pf.ID).toString()].arr sIter");
    var i4 = qs.createIndex("cIndex", IndexType.FUNCTIONAL,
        "pf.collectionHolderMap[(pf.ID).toString()].arr[pf.ID]", SEPARATOR + "portfolio pf");
    var i5 = qs.createIndex("inIndex", IndexType.FUNCTIONAL, "kIter.secId",
        SEPARATOR + "portfolio['0'].positions.values kIter");
    var i6 = qs.createIndex("sIndex", IndexType.FUNCTIONAL, "pos.secId",
        SEPARATOR + "portfolio.values val, val.positions.values pos");
    var i7 = qs.createIndex("p1Index", IndexType.PRIMARY_KEY, "pkid", SEPARATOR + "portfolio pf");
    var i8 = qs.createIndex("p2Index", IndexType.PRIMARY_KEY, "pk", SEPARATOR + "portfolio pf");
    if (!i1.getCanonicalizedFromClause()
        .equals(SEPARATOR + "portfolio index_iter1, index_iter1.positions.values index_iter2")
        || !i1.getCanonicalizedIndexedExpression().equals("index_iter2.secId")
        || !i1.getFromClause().equals(SEPARATOR + "portfolio pf, pf.positions.values vals")
        || !i1.getIndexedExpression().equals("vals.secId")) {
      fail("Mismatch found among fromClauses or IndexedExpressions");
    }
    if (!i2.getCanonicalizedFromClause().equals(SEPARATOR + "portfolio index_iter1")
        || !i2.getCanonicalizedIndexedExpression().equals("index_iter1.getCW(index_iter1.ID)")
        || !i2.getFromClause().equals(SEPARATOR + "portfolio pf")
        || !i2.getIndexedExpression().equals("pf.getCW(pf.ID)")) {
      fail("Mismatch found among fromClauses or IndexedExpressions");
    }
    if (!i3.getCanonicalizedFromClause().equals(
        SEPARATOR
            + "portfolio index_iter1, index_iter1.collectionHolderMap[index_iter1.ID.toString()].arr index_iter3")
        || !i3.getCanonicalizedIndexedExpression().equals("index_iter3")
        || !i3.getFromClause()
            .equals(
                SEPARATOR + "portfolio pf, pf.collectionHolderMap[(pf.ID).toString()].arr sIter")
        || !i3.getIndexedExpression().equals("sIter")) {
      fail("Mismatch found among fromClauses or IndexedExpressions");
    }
    if (!i4.getCanonicalizedFromClause().equals(SEPARATOR + "portfolio index_iter1")
        || !i4.getCanonicalizedIndexedExpression().equals(
            "index_iter1.collectionHolderMap[index_iter1.ID.toString()].arr[index_iter1.ID]")
        || !i4.getFromClause().equals(SEPARATOR + "portfolio pf") || !i4.getIndexedExpression()
            .equals("pf.collectionHolderMap[(pf.ID).toString()].arr[pf.ID]")) {
      fail("Mismatch found among fromClauses or IndexedExpressions");
    }
    if (!i5.getCanonicalizedFromClause()
        .equals(SEPARATOR + "portfolio['0'].positions.values index_iter4")
        || !i5.getCanonicalizedIndexedExpression().equals("index_iter4.secId")
        || !i5.getFromClause().equals(SEPARATOR + "portfolio['0'].positions.values kIter")
        || !i5.getIndexedExpression().equals("kIter.secId")) {
      fail("Mismatch found among fromClauses or IndexedExpressions");
    }
    if (!i6.getCanonicalizedFromClause()
        .equals(
            SEPARATOR + "portfolio.values index_iter5, index_iter5.positions.values index_iter6")
        || !i6.getCanonicalizedIndexedExpression().equals("index_iter6.secId")
        || !i6.getFromClause().equals(SEPARATOR + "portfolio.values val, val.positions.values pos")
        || !i6.getIndexedExpression().equals("pos.secId")) {
      fail("Mismatch found among fromClauses or IndexedExpressions");
    }
    if (!i7.getCanonicalizedFromClause().equals(SEPARATOR + "portfolio index_iter1")
        || !i7.getCanonicalizedIndexedExpression().equals("index_iter1.pkid")
        || !i7.getFromClause().equals(SEPARATOR + "portfolio pf")
        || !i7.getIndexedExpression().equals("pkid")) {
      fail("Mismatch found among fromClauses or IndexedExpressions");
    }
    if (!i8.getCanonicalizedFromClause().equals(SEPARATOR + "portfolio index_iter1")
        || !i8.getCanonicalizedIndexedExpression().equals("index_iter1.pk")
        || !i8.getFromClause().equals(SEPARATOR + "portfolio pf")
        || !i8.getIndexedExpression().equals("pk")) {
      fail("Mismatch found among fromClauses or IndexedExpressions");
    }
    qs.removeIndex(i1);
    qs.removeIndex(i2);
    qs.removeIndex(i3);
    qs.removeIndex(i4);
    qs.removeIndex(i5);
    qs.removeIndex(i6);
    qs.removeIndex(i7);
    qs.removeIndex(i8);
    var i9 =
        qs.createIndex("p3Index", IndexType.PRIMARY_KEY, "getPk", SEPARATOR + "portfolio pf");
    if (!i9.getCanonicalizedFromClause().equals(SEPARATOR + "portfolio index_iter1")
        || !i9.getCanonicalizedIndexedExpression().equals("index_iter1.pk")
        || !i9.getFromClause().equals(SEPARATOR + "portfolio pf")
        || !i9.getIndexedExpression().equals("getPk")) {
      fail("Mismatch found among fromClauses or IndexedExpressions");
    }
    qs.removeIndex(i9);
  }

  @Test
  public void numberOfIndexValuesStatIsUpdatedWhenEntryAdded() throws Exception {
    CacheUtils.log(((CompactRangeIndex) index).dump());
    var stats = index.getStatistics();
    assertEquals(4, stats.getNumberOfValues());
    region.put("4", new Portfolio(4));
    CacheUtils.log(((CompactRangeIndex) index).dump());
    stats = index.getStatistics();
    assertEquals(5, stats.getNumberOfValues());
    var results = region.query("status = 'active'");
    assertEquals(3, results.size());
  }

  /**
   * Needed to fix this since the key "4" was not found and the size was then redueced to 3 after
   * invalidating the key 3
   */
  @Test
  public void numberOfIndexValuesStatIsUpdatedWhenEntryInvalidated() throws Exception {
    var stats = index.getStatistics();
    region.invalidate("3");
    assertEquals(3, stats.getNumberOfValues());
    var results = region.query("status = 'active'");
    assertEquals(2, results.size());
  }

  @Test
  public void numberOfIndexValuesStatIsUpdatedWhenEntryDestroyed() throws Exception {
    var stats = index.getStatistics();
    region.put("4", new Portfolio(4));
    region.destroy("4");
    assertEquals(4, stats.getNumberOfValues());
    var results = region.query("status = 'active'");
    assertEquals(2, results.size());
  }

  // This test has a meaning only for Trunk code as it checks for Map implementation
  // Tests for Region clear operations on Index in a Local VM
  @Test
  public void indexIsUsedForQueryWhenRegionIsEmpty() {
    try {
      CacheUtils.getCache();
      isInitDone = false;
      var q =
          qs.newQuery("SELECT DISTINCT * FROM " + SEPARATOR + "portfolio where status = 'active'");
      QueryObserverHolder.setInstance(new QueryObserverAdapter() {

        @Override
        public void afterIndexLookup(Collection coll) {
          indexUsed = true;
        }
      });
      var set = (SelectResults) q.execute();
      if (set.size() == 0 || !indexUsed) {
        fail("Either Size of the result set is zero or Index is not used ");
      }
      indexUsed = false;

      region.clear();
      set = (SelectResults) q.execute();
      if (set.size() != 0 || !indexUsed) {
        fail("Either Size of the result set is not zero or Index is not used ");
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    } finally {
      isInitDone = false;
      CacheUtils.restartCache();
    }
  }

  // Asif : Tests for Region clear operations on Index in a Local VM for cases
  // when a clear
  // operation & region put operation occur concurrentlty
  @Test
  public void testConcurrentMapClearAndRegionPutOperation() {
    try {
      CacheUtils.getCache();
      isInitDone = false;
      var q =
          qs.newQuery("SELECT DISTINCT * FROM " + SEPARATOR + "portfolio where status = 'active'");
      QueryObserverHolder.setInstance(new QueryObserverAdapter() {

        @Override
        public void afterIndexLookup(Collection coll) {
          indexUsed = true;
        }

        @Override
        public void beforeRerunningIndexCreationQuery() {
          // Spawn a separate thread here which does a put opertion on region
          var th = new Thread(() -> {
            // Assert that the size of region is now 0
            assertTrue(region.size() == 0);
            region.put("" + 8, new Portfolio(8));
          });
          th.start();
          ThreadUtils.join(th, 30 * 1000);
          assertTrue(region.size() == 1);
        }
      });
      var set = (SelectResults) q.execute();
      if (set.size() == 0 || !indexUsed) {
        fail("Either Size of the result set is zero or Index is not used ");
      }
      indexUsed = false;
      region.clear();
      set = (SelectResults) q.execute();
      if (set.size() != 1 || !indexUsed) {
        fail("Either Size of the result set is not one or Index is not used ");
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    } finally {
      isInitDone = false;
      CacheUtils.restartCache();
    }
  }

  /**
   * Test to compare range and compact index. They should return the same results.
   */
  @Test
  public void rangeAndCompactIndexesShouldReturnTheSameResults() {
    try {
      // CacheUtils.restartCache();
      if (!isInitDone) {
        isInitDone = true;
      }
      qs.removeIndexes();

      var queryStr =
          new String[] {"Select status from " + SEPARATOR + "portfolio pf where status='active'",
              "Select pf.ID from " + SEPARATOR + "portfolio pf where pf.ID > 2 and pf.ID < 100",
              "Select * from " + SEPARATOR + "portfolio pf where pf.position1.secId > '2'",};

      var queryFields = new String[] {"status", "ID", "position1.secId",};

      for (var i = 0; i < queryStr.length; i++) {
        // Clear indexes if any.
        qs.removeIndexes();

        // initialize region.
        region.clear();
        for (var k = 0; k < 10; k++) {
          region.put("" + k, new Portfolio(k));
        }

        for (var j = 0; j < 1; j++) { // With different region size.
          // Update Region.
          for (var k = 0; k < (j * 100); k++) {
            region.put("" + k, new Portfolio(k));
          }

          // Create compact index.
          IndexManager.TEST_RANGEINDEX_ONLY = false;
          index = (IndexProtocol) qs.createIndex(queryFields[i] + "Index", IndexType.FUNCTIONAL,
              queryFields[i], SEPARATOR + "portfolio");

          // Execute Query.
          var rs = new SelectResults[1][2];
          var query = qs.newQuery(queryStr[i]);
          rs[0][0] = (SelectResults) query.execute();

          // remove compact index.
          qs.removeIndexes();

          // Create Range Index.
          IndexManager.TEST_RANGEINDEX_ONLY = true;
          index = (IndexProtocol) qs.createIndex(queryFields[i] + "rIndex", IndexType.FUNCTIONAL,
              queryFields[i], SEPARATOR + "portfolio");

          query = qs.newQuery(queryStr[i]);
          rs[0][1] = (SelectResults) query.execute();

          CacheUtils.log(
              "#### rs1 size is : " + (rs[0][0]).size() + " rs2 size is : " + (rs[0][1]).size());
          var ssORrs = new StructSetOrResultsSet();
          ssORrs.CompareQueryResultsWithoutAndWithIndexes(rs, 1, queryStr);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Test failed due to exception=" + e);
    } finally {
      IndexManager.TEST_RANGEINDEX_ONLY = false;
      isInitDone = false;
      CacheUtils.restartCache();
    }
  }

  /**
   * Ending of section - moved from wrongly spelled old class "IndexMaintainceJUnitTest". GEODE-1505
   */

  private void validateIndexForKeys(CompactRangeIndex ri) {
    assertEquals(6, ri.getIndexStorage().size());
    CloseableIterator<IndexStoreEntry> itr = null;
    try {
      itr = ri.getIndexStorage().iterator(null);
      while (itr.hasNext()) {
        var reEntry = itr.next();
        var obj = reEntry.getDeserializedRegionKey();
        assertTrue(obj instanceof String);
        assertTrue(idSet.contains(obj));
      }
    } finally {
      if (itr != null) {
        itr.close();
      }
    }
  }

  private void validateIndexForEntries(CompactRangeIndex ri) {
    assertEquals(6, ri.getIndexStorage().size());
    Iterator itr = ri.getIndexStorage().iterator(null);
    while (itr.hasNext()) {
      var obj = itr.next();
      assertFalse(obj instanceof Collection);
      var re = (MemoryIndexStoreEntry) obj;
      var pf = (Portfolio) re.getRegionEntry().getValueInVM((LocalRegion) ri.getRegion());
      assertTrue(idSet.contains(String.valueOf(pf.getID())));
    }
  }

  private void validateIndexForValues(CompactRangeIndex ri) {
    assertEquals(6, ri.getIndexStorage().size());
    CloseableIterator<IndexStoreEntry> itr = null;
    try {
      itr = ri.getIndexStorage().iterator(null);
      while (itr.hasNext()) {
        Object regionEntries = itr.next();
        assertTrue(regionEntries instanceof IndexStoreEntry);
      }
    } finally {
      if (itr != null) {
        itr.close();
      }
    }
  }

  private static class MapKeyIndexData implements Serializable {
    int id;
    public Map maap = new HashMap();

    public MapKeyIndexData(int id) {
      this.id = id;
    }

    public void addKeyValue(Object key, Object value) {
      maap.put(key, value);
    }
  }
}
