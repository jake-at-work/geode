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
 * Written by Doug Lea with assistance from members of JCP JSR-166 Expert Group and released to the
 * public domain, as explained at http://creativecommons.org/licenses/publicdomain Other
 * contributors include Andrew Wright, Jeffrey Hayes, Pat Fisher, Mike Judd.
 */
package org.apache.geode.internal.util.concurrent.cm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.junit.Test;

import org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap;
import org.apache.geode.util.JSR166TestCase;

public class ConcurrentHashMapJUnitTest extends JSR166TestCase { // TODO: reformat

  /**
   * Create a map from Integers 1-5 to Strings "A"-"E".
   */
  private static CustomEntryConcurrentHashMap map5() {
    var map = new CustomEntryConcurrentHashMap(5);
    assertTrue(map.isEmpty());
    map.put(one, "A");
    map.put(two, "B");
    map.put(three, "C");
    map.put(four, "D");
    map.put(five, "E");
    assertFalse(map.isEmpty());
    assertEquals(5, map.size());
    return map;
  }

  /**
   * clear removes all pairs
   */
  @Test
  public void testClear() {
    var map = map5();
    map.clear();
    assertEquals(map.size(), 0);
  }

  /**
   * Maps with same contents are equal
   */
  @Test
  public void testEquals() {
    var map1 = map5();
    var map2 = map5();
    assertEquals(map1, map2);
    assertEquals(map2, map1);
    map1.clear();
    assertFalse(map1.equals(map2));
    assertFalse(map2.equals(map1));
  }

  /**
   * contains returns true for contained value
   */
  @Test
  public void testContains() {
    var map = map5();
    assertTrue(map.contains("A"));
    assertFalse(map.contains("Z"));
  }

  /**
   * containsKey returns true for contained key
   */
  @Test
  public void testContainsKey() {
    var map = map5();
    assertTrue(map.containsKey(one));
    assertFalse(map.containsKey(zero));
  }

  /**
   * containsValue returns true for held values
   */
  @Test
  public void testContainsValue() {
    var map = map5();
    assertTrue(map.containsValue("A"));
    assertFalse(map.containsValue("Z"));
  }

  /**
   * enumeration returns an enumeration containing the correct elements
   */
  @Test
  public void testEnumeration() {
    var map = map5();
    var e = map.elements();
    var count = 0;
    while (e.hasMoreElements()) {
      count++;
      e.nextElement();
    }
    assertEquals(5, count);
  }

  /**
   * get returns the correct element at the given key, or null if not present
   */
  @Test
  public void testGet() {
    var map = map5();
    assertEquals("A", map.get(one));
    var empty = new CustomEntryConcurrentHashMap();
    assertNull(map.get("anything"));
  }

  /**
   * isEmpty is true of empty map and false for non-empty
   */
  @Test
  public void testIsEmpty() {
    var empty = new CustomEntryConcurrentHashMap();
    var map = map5();
    assertTrue(empty.isEmpty());
    assertFalse(map.isEmpty());
  }

  /**
   * keys returns an enumeration containing all the keys from the map
   */
  @Test
  public void testKeys() {
    var map = map5();
    var e = map.keys();
    var count = 0;
    while (e.hasMoreElements()) {
      count++;
      e.nextElement();
    }
    assertEquals(5, count);
  }

  /**
   * keySet returns a Set containing all the keys
   */
  @Test
  public void testKeySet() {
    var map = map5();
    var s = map.keySet();
    assertEquals(5, s.size());
    assertTrue(s.contains(one));
    assertTrue(s.contains(two));
    assertTrue(s.contains(three));
    assertTrue(s.contains(four));
    assertTrue(s.contains(five));
  }

  /**
   * keySet.toArray returns contains all keys
   */
  @Test
  public void testKeySetToArray() {
    var map = map5();
    var s = map.keySet();
    var ar = s.toArray();
    assertTrue(s.containsAll(Arrays.asList(ar)));
    assertEquals(5, ar.length);
    ar[0] = m10;
    assertFalse(s.containsAll(Arrays.asList(ar)));
  }

  /**
   * Values.toArray contains all values
   */
  @Test
  public void testValuesToArray() {
    var map = map5();
    var v = map.values();
    var ar = v.toArray();
    var s = new ArrayList(Arrays.asList(ar));
    assertEquals(5, ar.length);
    assertTrue(s.contains("A"));
    assertTrue(s.contains("B"));
    assertTrue(s.contains("C"));
    assertTrue(s.contains("D"));
    assertTrue(s.contains("E"));
  }

  /**
   * entrySet.toArray contains all entries
   */
  @Test
  public void testEntrySetToArray() {
    var map = map5();
    var s = map.entrySet();
    var ar = s.toArray();
    assertEquals(5, ar.length);
    for (var i = 0; i < 5; ++i) {
      assertTrue(map.containsKey(((Map.Entry) (ar[i])).getKey()));
      assertTrue(map.containsValue(((Map.Entry) (ar[i])).getValue()));
    }
  }

  /**
   * values collection contains all values
   */
  @Test
  public void testValues() {
    var map = map5();
    var s = map.values();
    assertEquals(5, s.size());
    assertTrue(s.contains("A"));
    assertTrue(s.contains("B"));
    assertTrue(s.contains("C"));
    assertTrue(s.contains("D"));
    assertTrue(s.contains("E"));
  }

  /**
   * entrySet contains all pairs
   */
  @Test
  public void testEntrySet() {
    var map = map5();
    var s = map.entrySet();
    assertEquals(5, s.size());
    for (final var o : s) {
      var e = (Map.Entry) o;
      assertTrue((e.getKey().equals(one) && e.getValue().equals("A"))
          || (e.getKey().equals(two) && e.getValue().equals("B"))
          || (e.getKey().equals(three) && e.getValue().equals("C"))
          || (e.getKey().equals(four) && e.getValue().equals("D"))
          || (e.getKey().equals(five) && e.getValue().equals("E")));
    }
  }

  /**
   * putAll adds all key-value pairs from the given map
   */
  @Test
  public void testPutAll() {
    var empty = new CustomEntryConcurrentHashMap();
    var map = map5();
    empty.putAll(map);
    assertEquals(5, empty.size());
    assertTrue(empty.containsKey(one));
    assertTrue(empty.containsKey(two));
    assertTrue(empty.containsKey(three));
    assertTrue(empty.containsKey(four));
    assertTrue(empty.containsKey(five));
  }

  /**
   * putIfAbsent works when the given key is not present
   */
  @Test
  public void testPutIfAbsent() {
    var map = map5();
    map.putIfAbsent(six, "Z");
    assertTrue(map.containsKey(six));
  }

  /**
   * putIfAbsent does not add the pair if the key is already present
   */
  @Test
  public void testPutIfAbsent2() {
    var map = map5();
    assertEquals("A", map.putIfAbsent(one, "Z"));
  }

  /**
   * replace fails when the given key is not present
   */
  @Test
  public void testReplace() {
    var map = map5();
    assertNull(map.replace(six, "Z"));
    assertFalse(map.containsKey(six));
  }

  /**
   * replace succeeds if the key is already present
   */
  @Test
  public void testReplace2() {
    var map = map5();
    assertNotNull(map.replace(one, "Z"));
    assertEquals("Z", map.get(one));
  }


  /**
   * replace value fails when the given key not mapped to expected value
   */
  @Test
  public void testReplaceValue() {
    var map = map5();
    assertEquals("A", map.get(one));
    assertFalse(map.replace(one, "Z", "Z"));
    assertEquals("A", map.get(one));
  }

  /**
   * replace value succeeds when the given key mapped to expected value
   */
  @Test
  public void testReplaceValue2() {
    var map = map5();
    assertEquals("A", map.get(one));
    assertTrue(map.replace(one, "A", "Z"));
    assertEquals("Z", map.get(one));
  }


  /**
   * remove removes the correct key-value pair from the map
   */
  @Test
  public void testRemove() {
    var map = map5();
    map.remove(five);
    assertEquals(4, map.size());
    assertFalse(map.containsKey(five));
  }

  /**
   * remove(key,value) removes only if pair present
   */
  @Test
  public void testRemove2() {
    var map = map5();
    map.remove(five, "E");
    assertEquals(4, map.size());
    assertFalse(map.containsKey(five));
    map.remove(four, "A");
    assertEquals(4, map.size());
    assertTrue(map.containsKey(four));

  }

  /**
   * size returns the correct values
   */
  @Test
  public void testSize() {
    var map = map5();
    var empty = new CustomEntryConcurrentHashMap();
    assertEquals(0, empty.size());
    assertEquals(5, map.size());
  }

  /**
   * toString contains toString of elements
   */
  @Test
  public void testToString() {
    var map = map5();
    var s = map.toString();
    for (var i = 1; i <= 5; ++i) {
      assertTrue(s.indexOf(String.valueOf(i)) >= 0);
    }
  }

  // Exception tests

  /**
   * Cannot create with negative capacity
   */
  @Test
  public void testConstructor1() {
    try {
      new CustomEntryConcurrentHashMap(-1, 0, 1);
      shouldThrow();
    } catch (IllegalArgumentException ignored) {
    }
  }

  /**
   * Cannot create with negative concurrency level
   */
  @Test
  public void testConstructor2() {
    try {
      new CustomEntryConcurrentHashMap(1, 0, -1);
      shouldThrow();
    } catch (IllegalArgumentException ignored) {
    }
  }

  /**
   * Cannot create with only negative capacity
   */
  @Test
  public void testConstructor3() {
    try {
      new CustomEntryConcurrentHashMap(-1);
      shouldThrow();
    } catch (IllegalArgumentException ignored) {
    }
  }

  /**
   * get(null) throws NPE
   */
  @Test
  public void testGet_NullPointerException() {
    try {
      var c = new CustomEntryConcurrentHashMap(5);
      c.get(null);
      shouldThrow();
    } catch (NullPointerException ignored) {
    }
  }

  /**
   * containsKey(null) throws NPE
   */
  @Test
  public void testContainsKey_NullPointerException() {
    try {
      var c = new CustomEntryConcurrentHashMap(5);
      c.containsKey(null);
      shouldThrow();
    } catch (NullPointerException ignored) {
    }
  }

  /**
   * containsValue(null) throws NPE
   */
  @Test
  public void testContainsValue_NullPointerException() {
    try {
      var c = new CustomEntryConcurrentHashMap(5);
      c.containsValue(null);
      shouldThrow();
    } catch (NullPointerException ignored) {
    }
  }

  /**
   * contains(null) throws NPE
   */
  @Test
  public void testContains_NullPointerException() {
    try {
      var c = new CustomEntryConcurrentHashMap(5);
      c.contains(null);
      shouldThrow();
    } catch (NullPointerException ignored) {
    }
  }

  /**
   * put(null,x) throws NPE
   */
  @Test
  public void testPut1_NullPointerException() {
    try {
      var c = new CustomEntryConcurrentHashMap(5);
      c.put(null, "whatever");
      shouldThrow();
    } catch (NullPointerException ignored) {
    }
  }

  /**
   * put(x, null) throws NPE
   */
  @Test
  public void testPut2_NullPointerException() {
    try {
      var c = new CustomEntryConcurrentHashMap(5);
      c.put("whatever", null);
      shouldThrow();
    } catch (NullPointerException ignored) {
    }
  }

  /**
   * putIfAbsent(null, x) throws NPE
   */
  @Test
  public void testPutIfAbsent1_NullPointerException() {
    try {
      var c = new CustomEntryConcurrentHashMap(5);
      c.putIfAbsent(null, "whatever");
      shouldThrow();
    } catch (NullPointerException ignored) {
    }
  }

  /**
   * replace(null, x) throws NPE
   */
  @Test
  public void testReplace_NullPointerException() {
    try {
      var c = new CustomEntryConcurrentHashMap(5);
      c.replace(null, "whatever");
      shouldThrow();
    } catch (NullPointerException ignored) {
    }
  }

  /**
   * replace(null, x, y) throws NPE
   */
  @Test
  public void testReplaceValue_NullPointerException() {
    try {
      var c = new CustomEntryConcurrentHashMap(5);
      c.replace(null, one, "whatever");
      shouldThrow();
    } catch (NullPointerException ignored) {
    }
  }

  /**
   * putIfAbsent(x, null) throws NPE
   */
  @Test
  public void testPutIfAbsent2_NullPointerException() {
    try {
      var c = new CustomEntryConcurrentHashMap(5);
      c.putIfAbsent("whatever", null);
      shouldThrow();
    } catch (NullPointerException ignored) {
    }
  }


  /**
   * replace(x, null) throws NPE
   */
  @Test
  public void testReplace2_NullPointerException() {
    try {
      var c = new CustomEntryConcurrentHashMap(5);
      c.replace("whatever", null);
      shouldThrow();
    } catch (NullPointerException ignored) {
    }
  }

  /**
   * replace(x, null, y) throws NPE
   */
  @Test
  public void testReplaceValue2_NullPointerException() {
    try {
      var c = new CustomEntryConcurrentHashMap(5);
      c.replace("whatever", null, "A");
      shouldThrow();
    } catch (NullPointerException ignored) {
    }
  }

  /**
   * replace(x, y, null) throws NPE
   */
  @Test
  public void testReplaceValue3_NullPointerException() {
    try {
      var c = new CustomEntryConcurrentHashMap(5);
      c.replace("whatever", one, null);
      shouldThrow();
    } catch (NullPointerException ignored) {
    }
  }


  /**
   * remove(null) throws NPE
   */
  @Test
  public void testRemove1_NullPointerException() {
    try {
      var c = new CustomEntryConcurrentHashMap(5);
      c.put("sadsdf", "asdads");
      c.remove(null);
      shouldThrow();
    } catch (NullPointerException ignored) {
    }
  }

  /**
   * remove(null, x) throws NPE
   */
  @Test
  public void testRemove2_NullPointerException() {
    try {
      var c = new CustomEntryConcurrentHashMap(5);
      c.put("sadsdf", "asdads");
      c.remove(null, "whatever");
      shouldThrow();
    } catch (NullPointerException ignored) {
    }
  }

  /**
   * remove(x, null) returns false
   */
  @Test
  public void testRemove3() {
    try {
      var c = new CustomEntryConcurrentHashMap(5);
      c.put("sadsdf", "asdads");
      assertFalse(c.remove("sadsdf", null));
    } catch (NullPointerException e) {
      fail();
    }
  }

  /**
   * A deserialized map equals original
   */
  @Test
  public void testSerialization() {
    var q = map5();

    try {
      var bout = new ByteArrayOutputStream(10000);
      var out = new ObjectOutputStream(new BufferedOutputStream(bout));
      out.writeObject(q);
      out.close();

      var bin = new ByteArrayInputStream(bout.toByteArray());
      var in = new ObjectInputStream(new BufferedInputStream(bin));
      var r = (CustomEntryConcurrentHashMap) in.readObject();
      assertEquals(q.size(), r.size());
      assertTrue(q.equals(r));
      assertTrue(r.equals(q));
    } catch (Exception e) {
      e.printStackTrace();
      unexpectedException();
    }
  }


  /**
   * SetValue of an EntrySet entry sets value in the map.
   */
  @Test
  public void testSetValueWriteThrough() {
    // Adapted from a bug report by Eric Zoerner
    var map = new CustomEntryConcurrentHashMap(2, 5.0f, 1);
    assertTrue(map.isEmpty());
    for (var i = 0; i < 20; i++) {
      map.put(i, i);
    }
    assertFalse(map.isEmpty());
    var entry1 = (Map.Entry) map.entrySet().iterator().next();

    // assert that entry1 is not 16
    assertTrue("entry is 16, test not valid", !entry1.getKey().equals(16));

    // remove 16 (a different key) from map
    // which just happens to cause entry1 to be cloned in map
    map.remove(16);
    entry1.setValue("XYZ");
    assertTrue(map.containsValue("XYZ")); // fails
  }
}
