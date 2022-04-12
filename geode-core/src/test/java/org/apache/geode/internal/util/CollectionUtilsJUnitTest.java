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
package org.apache.geode.internal.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;


/**
 * The CollectionUtilsJUnitTest class is a test suite of test cases testing the contract and
 * functionality of the CollectionUtils class.
 * <p/>
 *
 * @see org.apache.geode.internal.util.CollectionUtils
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since GemFire 7.0
 */
public class CollectionUtilsJUnitTest {

  @Test
  public void testAsList() {
    final Integer[] numbers = {0, 1, 2, 1, 0};

    final var numberList = CollectionUtils.asList(numbers);

    assertNotNull(numberList);
    assertFalse(numberList.isEmpty());
    assertEquals(numbers.length, numberList.size());
    assertTrue(numberList.containsAll(Arrays.asList(numbers)));
    assertEquals(new Integer(0), numberList.remove(0));
    assertEquals(numbers.length - 1, numberList.size());
  }

  @Test
  public void testAsSet() {
    final Integer[] numbers = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

    final var numberSet = CollectionUtils.asSet(numbers);

    assertNotNull(numberSet);
    assertFalse(numberSet.isEmpty());
    assertEquals(numbers.length, numberSet.size());
    assertTrue(numberSet.containsAll(Arrays.asList(numbers)));
    assertTrue(numberSet.remove(1));
    assertEquals(numbers.length - 1, numberSet.size());
  }

  @Test
  public void testAsSetWithNonUniqueElements() {
    final Integer[] numbers = {0, 1, 2, 1, 0};

    final var numberSet = CollectionUtils.asSet(numbers);

    assertNotNull(numberSet);
    assertFalse(numberSet.isEmpty());
    assertEquals(3, numberSet.size());
    assertTrue(numberSet.containsAll(Arrays.asList(numbers)));
  }

  @Test
  public void testCreateProperties() {
    var properties =
        CollectionUtils.createProperties(Collections.singletonMap("one", "two"));

    assertNotNull(properties);
    assertFalse(properties.isEmpty());
    assertTrue(properties.containsKey("one"));
    assertEquals("two", properties.getProperty("one"));
  }

  @Test
  public void testCreateMultipleProperties() {
    Map<String, String> map = new HashMap<>(3);

    map.put("one", "A");
    map.put("two", "B");
    map.put("six", "C");

    var properties = CollectionUtils.createProperties(map);

    assertNotNull(properties);
    assertFalse(properties.isEmpty());
    assertEquals(map.size(), properties.size());

    for (var entry : map.entrySet()) {
      assertTrue(properties.containsKey(entry.getKey()));
      assertEquals(entry.getValue(), properties.get(entry.getKey()));
    }
  }

  @Test
  public void testCreateEmptyProperties() {
    var properties = CollectionUtils.createProperties(null);

    assertNotNull(properties);
    assertTrue(properties.isEmpty());
  }

  @Test
  public void testEmptyListWithNullList() {
    final var actualList = CollectionUtils.emptyList(null);

    assertNotNull(actualList);
    assertTrue(actualList.isEmpty());
  }

  @Test
  public void testEmptyListWithEmptyList() {
    final List<Object> expectedList = new ArrayList<>(0);

    assertNotNull(expectedList);
    assertTrue(expectedList.isEmpty());

    final var actualList = CollectionUtils.emptyList(expectedList);

    assertSame(expectedList, actualList);
  }

  @Test
  public void testEmptyListWithList() {
    final var expectedList =
        Arrays.asList("aardvark", "baboon", "cat", "dog", "eel", "ferret");

    assertNotNull(expectedList);
    assertFalse(expectedList.isEmpty());

    final var actualList = CollectionUtils.emptyList(expectedList);

    assertSame(expectedList, actualList);
  }

  @Test
  public void testEmptySetWithNullSet() {
    final var actualSet = CollectionUtils.emptySet(null);

    assertNotNull(actualSet);
    assertTrue(actualSet.isEmpty());
  }

  @Test
  public void testEmptySetWithEmptySet() {
    final Set<Object> expectedSet = new HashSet<>(0);

    assertNotNull(expectedSet);
    assertTrue(expectedSet.isEmpty());

    final var actualSet = CollectionUtils.emptySet(expectedSet);

    assertSame(expectedSet, actualSet);
  }

  @Test
  public void testEmptySetWithSet() {
    final Set<String> expectedSet =
        new HashSet<>(Arrays.asList("aardvark", "baboon", "cat", "dog", "ferret"));

    assertNotNull(expectedSet);
    assertFalse(expectedSet.isEmpty());

    final var actualSet = CollectionUtils.emptySet(expectedSet);

    assertSame(expectedSet, actualSet);
  }

  @Test
  public void testFindAll() {
    final var numbers = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 7, 8, 9);

    // accept all even numbers
    final var matches = CollectionUtils.findAll(numbers, number -> (number % 2 == 0));

    assertNotNull(matches);
    assertFalse(matches.isEmpty());
    assertTrue(matches.containsAll(Arrays.asList(0, 2, 4, 6, 8)));
  }

  @Test
  public void testFindAllWhenMultipleElementsMatch() {
    final var numbers = Arrays.asList(0, 1, 2, 1, 4, 1, 6, 1, 7, 1, 9);

    // accept 1
    final var matches = CollectionUtils.findAll(numbers, number -> (number == 1));

    assertNotNull(matches);
    assertEquals(5, matches.size());
    assertEquals(matches, Arrays.asList(1, 1, 1, 1, 1));
  }

  @Test
  public void testFindAllWhenNoElementsMatch() {
    final var numbers = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    // accept negative numbers
    final var matches = CollectionUtils.findAll(numbers, number -> (number < 0));

    assertNotNull(matches);
    assertTrue(matches.isEmpty());
  }

  @Test
  public void testFindBy() {
    final var numbers = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 7, 8, 9);

    // accept 2
    final var match = CollectionUtils.findBy(numbers, number -> (number == 2));

    assertNotNull(match);
    assertEquals(2, match.intValue());
  }

  @Test
  public void testFindByWhenMultipleElementsMatch() {
    final var numbers = Arrays.asList(0, 1, 2, 1, 4, 1, 6, 1, 7, 1, 9);

    // accept 1
    final var match = CollectionUtils.findBy(numbers, number -> (number == 1));

    assertNotNull(match);
    assertEquals(1, match.intValue());
  }

  @Test
  public void testFindByWhenNoElementsMatch() {
    final var numbers = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 7, 8, 9);

    // accept 10
    final var match = CollectionUtils.findBy(numbers, number -> (number == 10));

    assertNull(match);
  }

  @Test
  public void testRemoveKeys() {
    final Map<Object, String> expectedMap = new HashMap<>(6);

    expectedMap.put("key1", "value");
    expectedMap.put("key2", "null");
    expectedMap.put("key3", "nil");
    expectedMap.put("key4", null);
    expectedMap.put("key5", "");
    expectedMap.put("key6", "  ");

    assertFalse(expectedMap.isEmpty());
    assertEquals(6, expectedMap.size());

    final var actualMap =
        CollectionUtils.removeKeys(expectedMap, entry -> StringUtils.isNotBlank(entry.getValue()));

    assertSame(expectedMap, actualMap);
    assertFalse(actualMap.isEmpty());
    assertEquals(3, actualMap.size());
    assertTrue(actualMap.keySet().containsAll(Arrays.asList("key1", "key2", "key3")));
  }

  @Test
  public void testRemoveKeysWithNullValues() {
    final Map<Object, Object> expectedMap = new HashMap<>(3);

    expectedMap.put("one", "test");
    expectedMap.put("two", null);
    expectedMap.put(null, "null");
    expectedMap.put("null", "nil");

    assertFalse(expectedMap.isEmpty());
    assertEquals(4, expectedMap.size());

    final var actualMap = CollectionUtils.removeKeysWithNullValues(expectedMap);

    assertSame(expectedMap, actualMap);
    assertEquals(3, expectedMap.size());
    assertEquals("null", expectedMap.get(null));
  }

  @Test
  public void testRemoveKeysWithNullValuesFromEmptyMap() {
    final Map<?, ?> expectedMap = Collections.emptyMap();

    assertNotNull(expectedMap);
    assertTrue(expectedMap.isEmpty());

    final var actualMap = CollectionUtils.removeKeysWithNullValues(expectedMap);

    assertSame(expectedMap, actualMap);
    assertTrue(actualMap.isEmpty());
  }

  @Test
  public void testRemoveKeysWithNullValuesFromMapWithNoNullValues() {
    final Map<String, Object> map = new HashMap<>(5);

    map.put("one", "test");
    map.put("null", "null");
    map.put("two", "testing");
    map.put(null, "nil");
    map.put("three", "tested");

    assertEquals(5, map.size());

    CollectionUtils.removeKeysWithNullValues(map);

    assertEquals(5, map.size());
  }

  @Test
  public void testAddAllCollectionEnumerationWithList() {
    final var list = new ArrayList<String>(4);

    list.add("one");
    list.add("two");

    final var v = new Vector<String>();
    v.add("three");
    v.add("four");

    var modified = CollectionUtils.addAll(list, v.elements());

    assertTrue(modified);
    assertEquals(4, list.size());
    assertSame(v.get(0), list.get(2));
    assertSame(v.get(1), list.get(3));
  }

  @Test
  public void testAddAllCollectionEnumerationWithUnmodified() {
    final var set = new HashSet<String>();

    set.add("one");
    set.add("two");

    final var v = new Vector<String>();
    v.add("one");

    var modified = CollectionUtils.addAll(set, v.elements());

    assertTrue(!modified);
    assertEquals(2, set.size());
  }

  @Test
  public void testAddAllCollectionEnumerationWithEmpty() {
    final var list = new ArrayList<String>(2);

    list.add("one");
    list.add("two");

    var modified = CollectionUtils.addAll(list, new Enumeration<String>() {
      @Override
      public boolean hasMoreElements() {
        return false;
      }

      @Override
      public String nextElement() {
        throw new NoSuchElementException();
      }
    });

    assertTrue(!modified);
    assertEquals(2, list.size());
  }

  @Test
  public void testAddAllCollectionEnumerationWithNull() {
    final var list = new ArrayList<String>(2);

    list.add("one");
    list.add("two");

    var modified = CollectionUtils.addAll(list, null);

    assertTrue(!modified);
    assertEquals(2, list.size());
  }

  @Test
  public void testUnmodifiableIterable() {
    final var list = new ArrayList<Integer>();
    list.add(0);
    list.add(1);

    final var iterable = CollectionUtils.unmodifiableIterable(list);
    assertNotNull(iterable);

    final var iterator1 = iterable.iterator();
    assertNotNull(iterator1);
    assertTrue(iterator1.hasNext());
    assertSame(list.get(0), iterator1.next());
    assertSame(list.get(1), iterator1.next());
    assertTrue(!iterator1.hasNext());
    try {
      iterator1.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      // ignore
    }

    list.add(2);
    try {
      iterator1.next();
      fail("Expected ConcurrentModificationException");
    } catch (ConcurrentModificationException e) {
      // ignore
    }

    final var iterator2 = iterable.iterator();
    assertNotNull(iterator2);
    assertNotSame(iterator1, iterator2);
    assertTrue(iterator2.hasNext());
    assertSame(list.get(0), iterator2.next());
    assertSame(list.get(1), iterator2.next());
    assertSame(list.get(2), iterator2.next());
    assertTrue(!iterator2.hasNext());
    try {
      iterator2.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      // ignore
    }

    try {
      iterator2.remove();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // ignore
    }
  }
}
