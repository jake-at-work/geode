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
package org.apache.geode.cache.query.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.HashSet;
import java.util.NoSuchElementException;

import org.junit.Test;

import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.internal.types.ObjectTypeImpl;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.cache.query.types.StructType;
import org.apache.geode.internal.InternalDataSerializer;

/**
 * Test ResultsBag Limit behaviour
 *
 * TODO: Test for null behaviour in various functions
 */
public class ResultsBagLimitBehaviourJUnitTest {

  @Test
  public void testAsListAndAsSetMethod() {
    var bag = getBagObject(String.class);
    bag.add(wrap("one", bag.getCollectionType().getElementType()));
    bag.add(wrap("two", bag.getCollectionType().getElementType()));
    bag.add(wrap("three", bag.getCollectionType().getElementType()));
    bag.add(wrap("four", bag.getCollectionType().getElementType()));
    assertEquals(4, bag.size());
    bag.applyLimit(2);
    var list = bag.asList();
    assertEquals(2, list.size());
    var set = bag.asSet();
    assertEquals(2, set.size());
  }

  @Test
  public void testOccurrence() {
    var bag = getBagObject(String.class);
    bag.add(wrap(null, bag.getCollectionType().getElementType()));
    bag.add(wrap(null, bag.getCollectionType().getElementType()));
    bag.add(wrap("one", bag.getCollectionType().getElementType()));
    bag.add(wrap("two", bag.getCollectionType().getElementType()));
    bag.add(wrap("two", bag.getCollectionType().getElementType()));
    bag.add(wrap("three", bag.getCollectionType().getElementType()));
    bag.add(wrap("three", bag.getCollectionType().getElementType()));
    bag.add(wrap("three", bag.getCollectionType().getElementType()));
    bag.add(wrap("four", bag.getCollectionType().getElementType()));
    bag.add(wrap("four", bag.getCollectionType().getElementType()));
    bag.add(wrap("four", bag.getCollectionType().getElementType()));
    bag.add(wrap("four", bag.getCollectionType().getElementType()));
    assertEquals(12, bag.size());
    bag.applyLimit(6);
    var total = bag.occurrences(wrap(null, bag.getCollectionType().getElementType()));
    total += bag.occurrences(wrap("one", bag.getCollectionType().getElementType()));
    total += bag.occurrences(wrap("two", bag.getCollectionType().getElementType()));
    total += bag.occurrences(wrap("three", bag.getCollectionType().getElementType()));
    total += bag.occurrences(wrap("four", bag.getCollectionType().getElementType()));
    assertEquals(6, total);
  }

  @Test
  public void testIteratorType() {
    var bag = getBagObject(String.class);
    bag.add(wrap("one", bag.getCollectionType().getElementType()));
    bag.add(wrap("two", bag.getCollectionType().getElementType()));
    bag.add(wrap("two", bag.getCollectionType().getElementType()));
    bag.add(wrap("three", bag.getCollectionType().getElementType()));
    bag.add(wrap("three", bag.getCollectionType().getElementType()));
    assertTrue(bag.iterator() instanceof Bag.BagIterator);
    bag.applyLimit(6);
    assertTrue(bag.iterator() instanceof Bag.BagIterator);
    bag = getBagObject(String.class);
    bag.add(wrap("one", bag.getCollectionType().getElementType()));
    bag.add(wrap("two", bag.getCollectionType().getElementType()));
    bag.add(wrap("two", bag.getCollectionType().getElementType()));
    bag.add(wrap("three", bag.getCollectionType().getElementType()));
    bag.add(wrap("three", bag.getCollectionType().getElementType()));
    bag.add(wrap("three", bag.getCollectionType().getElementType()));
    bag.add(wrap("four", bag.getCollectionType().getElementType()));
    bag.add(wrap("four", bag.getCollectionType().getElementType()));
    bag.add(wrap("four", bag.getCollectionType().getElementType()));
    bag.add(wrap("four", bag.getCollectionType().getElementType()));
    bag.applyLimit(6);
    if (!(bag instanceof StructBag)) {
      assertTrue(bag.iterator() instanceof Bag.LimitBagIterator);
    }
  }

  @Test
  public void testContains() {
    var bag = getBagObject(Integer.class);
    bag.add(wrap(1, bag.getCollectionType().getElementType()));
    bag.add(wrap(2, bag.getCollectionType().getElementType()));
    bag.add(wrap(3, bag.getCollectionType().getElementType()));
    bag.add(wrap(null, bag.getCollectionType().getElementType()));
    bag.add(wrap(null, bag.getCollectionType().getElementType()));
    bag.add(wrap(4, bag.getCollectionType().getElementType()));
    bag.add(wrap(5, bag.getCollectionType().getElementType()));
    bag.add(wrap(6, bag.getCollectionType().getElementType()));
    bag.add(wrap(7, bag.getCollectionType().getElementType()));
    bag.add(wrap(8, bag.getCollectionType().getElementType()));
    bag.add(wrap(9, bag.getCollectionType().getElementType()));
    bag.add(wrap(10, bag.getCollectionType().getElementType()));
    bag.add(wrap(11, bag.getCollectionType().getElementType()));
    bag.add(wrap(12, bag.getCollectionType().getElementType()));
    bag.add(wrap(13, bag.getCollectionType().getElementType()));
    bag.add(wrap(14, bag.getCollectionType().getElementType()));
    bag.add(wrap(15, bag.getCollectionType().getElementType()));
    bag.add(wrap(16, bag.getCollectionType().getElementType()));
    bag.applyLimit(6);
    var temp = bag.asList();
    assertEquals(6, bag.size());
    for (var i = 1; i < 17; ++i) {
      Integer intg = i;
      assertTrue(temp.contains(wrap(intg, bag.getCollectionType().getElementType())) == bag
          .contains(wrap(intg, bag.getCollectionType().getElementType())));
    }
    assertTrue(temp.contains(wrap(null, bag.getCollectionType().getElementType())) == bag
        .contains(wrap(null, bag.getCollectionType().getElementType())));
  }

  @Test
  public void testAddExceptionIfLimitApplied() {
    var bag = getBagObject(String.class);
    bag.add(wrap("one", bag.getCollectionType().getElementType()));
    bag.add(wrap("two", bag.getCollectionType().getElementType()));
    bag.add(wrap("two", bag.getCollectionType().getElementType()));
    bag.applyLimit(6);
    try {
      bag.add(wrap("four", bag.getCollectionType().getElementType()));
      fail("Addition to bag with limit applied should have failed");
    } catch (UnsupportedOperationException soe) {
      // Expected exception
    }
  }

  // Internal method AddAndGetOccurrence used for iter evaluating
  // only up till the limit
  @Test
  public void testAddAndGetOccurrence() {
    var bag = getBagObject(String.class);
    bag = getBagObject(String.class);
    var elementType = bag.getCollectionType().getElementType();
    assertEquals(1, bag.addAndGetOccurence(elementType instanceof StructType
        ? ((Struct) wrap("one", elementType)).getFieldValues() : wrap("one", elementType)));
    bag.add(wrap("two", elementType));
    assertEquals(2,
        bag.addAndGetOccurence(
            elementType instanceof StructType ? ((Struct) wrap("two", elementType)).getFieldValues()
                : wrap("two", bag.getCollectionType().getElementType())));
    bag.add(wrap("three", bag.getCollectionType().getElementType()));
    bag.add(wrap("three", bag.getCollectionType().getElementType()));
    assertEquals(3, bag.addAndGetOccurence(elementType instanceof StructType
        ? ((Struct) wrap("three", elementType)).getFieldValues() : wrap("three", elementType)));
    bag.add(wrap(null, bag.getCollectionType().getElementType()));
    bag.add(wrap(null, bag.getCollectionType().getElementType()));
    assertEquals(3, bag.addAndGetOccurence(elementType instanceof StructType
        ? ((Struct) wrap(null, elementType)).getFieldValues() : wrap(null, elementType)));
  }

  @Test
  public void testSizeWithLimitApplied() {
    var bag = getBagObject(String.class);
    bag.add(wrap("two", bag.getCollectionType().getElementType()));
    bag.add(wrap("three", bag.getCollectionType().getElementType()));
    bag.add(wrap("three", bag.getCollectionType().getElementType()));
    bag.add(wrap("three", bag.getCollectionType().getElementType()));
    bag.add(wrap("three", bag.getCollectionType().getElementType()));
    assertEquals(5, bag.size());
    // Limit less than actual size
    bag.applyLimit(3);
    assertEquals(3, bag.size());
    bag = getBagObject(String.class);
    bag.add(wrap("two", bag.getCollectionType().getElementType()));
    bag.add(wrap("three", bag.getCollectionType().getElementType()));
    bag.applyLimit(7);
    assertEquals(2, bag.size());
  }

  @Test
  public void testRemove() {
    // Test when actual size in resultset is less than the limit
    var bag = getBagObject(String.class);
    bag.add(wrap(null, bag.getCollectionType().getElementType()));
    bag.add(wrap(null, bag.getCollectionType().getElementType()));
    bag.add(wrap("one", bag.getCollectionType().getElementType()));
    bag.add(wrap("two", bag.getCollectionType().getElementType()));
    bag.add(wrap("two", bag.getCollectionType().getElementType()));
    bag.add(wrap("three", bag.getCollectionType().getElementType()));
    bag.add(wrap("three", bag.getCollectionType().getElementType()));
    bag.add(wrap("three", bag.getCollectionType().getElementType()));
    bag.add(wrap("four", bag.getCollectionType().getElementType()));
    bag.add(wrap("four", bag.getCollectionType().getElementType()));
    bag.add(wrap("four", bag.getCollectionType().getElementType()));
    bag.add(wrap("four", bag.getCollectionType().getElementType()));
    bag.applyLimit(15);
    assertEquals(12, bag.size());
    bag.remove(wrap(null, bag.getCollectionType().getElementType()));
    assertEquals(11, bag.size());
    assertEquals(1, bag.occurrences(wrap(null, bag.getCollectionType().getElementType())));
    bag.remove(wrap("three", bag.getCollectionType().getElementType()));
    assertEquals(10, bag.size());
    assertEquals(2, bag.occurrences(wrap("three", bag.getCollectionType().getElementType())));
    // Test when the actual size is more than the limit
    bag = getBagObject(Integer.class);
    bag.add(wrap(null, bag.getCollectionType().getElementType()));
    bag.add(wrap(null, bag.getCollectionType().getElementType()));
    bag.add(wrap(1, bag.getCollectionType().getElementType()));
    bag.add(wrap(2, bag.getCollectionType().getElementType()));
    bag.add(wrap(2, bag.getCollectionType().getElementType()));
    bag.add(wrap(3, bag.getCollectionType().getElementType()));
    bag.add(wrap(3, bag.getCollectionType().getElementType()));
    bag.add(wrap(3, bag.getCollectionType().getElementType()));
    bag.add(wrap(4, bag.getCollectionType().getElementType()));
    bag.add(wrap(4, bag.getCollectionType().getElementType()));
    bag.add(wrap(4, bag.getCollectionType().getElementType()));
    bag.add(wrap(4, bag.getCollectionType().getElementType()));
    bag.applyLimit(8);
    var temp = bag.asList();
    var currSize = 8;
    assertEquals(currSize, 8);
    for (var i = 1; i < 5; ++i) {
      Integer intg = i;
      if (temp.contains(wrap(intg, bag.getCollectionType().getElementType()))) {
        var occurrence = bag.occurrences(wrap(intg, bag.getCollectionType().getElementType()));
        assertTrue(bag.remove(wrap(intg, bag.getCollectionType().getElementType())));
        assertEquals(--occurrence,
            bag.occurrences(wrap(intg, bag.getCollectionType().getElementType())));
        --currSize;
        assertEquals(currSize, bag.size());
      } else {
        assertEquals(0, bag.occurrences(wrap(intg, bag.getCollectionType().getElementType())));
      }
    }
    if (temp.contains(wrap(null, bag.getCollectionType().getElementType()))) {
      var occurrence = bag.occurrences(wrap(null, bag.getCollectionType().getElementType()));
      assertTrue(bag.remove(wrap(null, bag.getCollectionType().getElementType())));
      assertEquals(--occurrence,
          bag.occurrences(wrap(null, bag.getCollectionType().getElementType())));
      --currSize;
      assertEquals(currSize, bag.size());
    }

    // Test null removal
    bag = getBagObject(Object.class);
    for (var i = 0; i < 20; ++i) {
      bag.add(wrap(null, bag.getCollectionType().getElementType()));
    }
    bag.applyLimit(4);

    for (var i = 0; i < 3; ++i) {
      bag.remove(wrap(null, bag.getCollectionType().getElementType()));
    }

    assertEquals(1, bag.size());
    assertTrue(bag.contains(wrap(null, bag.getCollectionType().getElementType())));
    var itr = bag.iterator();
    assertEquals(wrap(null, bag.getCollectionType().getElementType()), itr.next());
    assertFalse(itr.hasNext());
    bag.remove(wrap(null, bag.getCollectionType().getElementType()));
    assertEquals(0, bag.size());
    assertFalse(bag.contains(wrap(null, bag.getCollectionType().getElementType())));
    itr = bag.iterator();
    assertFalse(itr.hasNext());

  }

  @Test
  public void testAddAllExceptionIfLimitApplied() {
    var bag = getBagObject(Object.class);
    bag.applyLimit(6);
    try {
      bag.addAll(new HashSet());
      fail("addAll invocation to bag with limit applied should have failed");
    } catch (UnsupportedOperationException soe) {
      // Expected exception
    }
  }

  @Test
  public void testToDataFromData() throws Exception {
    // Test with limit specified & limit less than internal size
    var toBag = getBagObject(String.class);
    toBag.add(wrap(null, toBag.getCollectionType().getElementType()));
    toBag.add(wrap(null, toBag.getCollectionType().getElementType()));
    toBag.add(wrap(null, toBag.getCollectionType().getElementType()));
    toBag.add(wrap("one", toBag.getCollectionType().getElementType()));
    toBag.add(wrap("two", toBag.getCollectionType().getElementType()));
    toBag.add(wrap("two", toBag.getCollectionType().getElementType()));
    toBag.add(wrap("three", toBag.getCollectionType().getElementType()));
    toBag.add(wrap("three", toBag.getCollectionType().getElementType()));
    toBag.add(wrap("three", toBag.getCollectionType().getElementType()));
    toBag.add(wrap("four", toBag.getCollectionType().getElementType()));
    toBag.add(wrap("four", toBag.getCollectionType().getElementType()));
    toBag.add(wrap("four", toBag.getCollectionType().getElementType()));
    toBag.add(wrap("four", toBag.getCollectionType().getElementType()));
    toBag.applyLimit(9);
    assertEquals(9, toBag.size());
    var baos = new ByteArrayOutputStream(10240);
    var dos = new DataOutputStream(baos);
    toBag.toData(dos, InternalDataSerializer.createSerializationContext(dos));
    var data = baos.toByteArray();
    var bis = new ByteArrayInputStream(data);
    var dis = new DataInputStream(bis);
    // Create a From ResultBag
    var fromBag = getBagObject(String.class);
    fromBag.fromData(dis, InternalDataSerializer.createDeserializationContext(dis));
    assertEquals(toBag.size(), fromBag.size());
    assertEquals(toBag.occurrences(wrap(null, toBag.getCollectionType().getElementType())),
        fromBag.occurrences(wrap(null, fromBag.getCollectionType().getElementType())));
    assertEquals(toBag.occurrences(wrap("one", toBag.getCollectionType().getElementType())),
        fromBag.occurrences(wrap("one", fromBag.getCollectionType().getElementType())));
    assertEquals(toBag.occurrences(wrap("two", toBag.getCollectionType().getElementType())),
        fromBag.occurrences(wrap("two", fromBag.getCollectionType().getElementType())));
    assertEquals(toBag.occurrences(wrap("three", toBag.getCollectionType().getElementType())),
        fromBag.occurrences(wrap("three", fromBag.getCollectionType().getElementType())));
    assertEquals(toBag.occurrences(wrap("four", toBag.getCollectionType().getElementType())),
        fromBag.occurrences(wrap("four", fromBag.getCollectionType().getElementType())));
    assertEquals(toBag.occurrences(wrap(null, toBag.getCollectionType().getElementType())),
        fromBag.occurrences(wrap(null, fromBag.getCollectionType().getElementType())));
    assertFalse(toBag.asList().retainAll(fromBag.asList()));
  }

  @Test
  public void testLimitResultsBagIterator_1() {
    var bag = getBagObject(Integer.class);
    bag.add(wrap(null, bag.getCollectionType().getElementType()));
    bag.add(wrap(null, bag.getCollectionType().getElementType()));
    bag.add(wrap(null, bag.getCollectionType().getElementType()));
    bag.add(wrap(1, bag.getCollectionType().getElementType()));
    bag.add(wrap(2, bag.getCollectionType().getElementType()));
    bag.add(wrap(2, bag.getCollectionType().getElementType()));
    bag.add(wrap(3, bag.getCollectionType().getElementType()));
    bag.add(wrap(3, bag.getCollectionType().getElementType()));
    bag.add(wrap(3, bag.getCollectionType().getElementType()));
    bag.add(wrap(4, bag.getCollectionType().getElementType()));
    bag.add(wrap(4, bag.getCollectionType().getElementType()));
    bag.add(wrap(4, bag.getCollectionType().getElementType()));
    bag.add(wrap(4, bag.getCollectionType().getElementType()));
    bag.add(wrap(5, bag.getCollectionType().getElementType()));
    bag.add(wrap(5, bag.getCollectionType().getElementType()));
    bag.add(wrap(5, bag.getCollectionType().getElementType()));
    bag.add(wrap(5, bag.getCollectionType().getElementType()));
    bag.add(wrap(5, bag.getCollectionType().getElementType()));
    bag.applyLimit(8);
    var itr = bag.iterator();
    if (!(bag instanceof StructBag)) {
      assertTrue(bag.iterator() instanceof Bag.LimitBagIterator);
    }
    var asList = bag.asList();

    // Check repetition
    var i = 0;
    while (itr.hasNext()) {
      assertEquals(itr.next(), asList.get(i++));
    }
    // Remvove first 5. This means after the completion of iteration the
    // size should be 3
    i = 0;
    itr = bag.iterator();
    while (i < 5) {
      itr.next();
      itr.remove();
      ++i;
    }

    assertEquals(3, bag.size());
    i = 0;
    itr = bag.iterator();
    while (i < 3) {
      assertEquals(asList.get(i + 5), itr.next());
      ++i;
    }
  }

  @Test
  public void testLimitResultsBagIterator_2() {
    var bag = getBagObject(Object.class);
    for (var i = 0; i < 20; ++i) {
      bag.add(wrap(null, bag.getCollectionType().getElementType()));
    }
    bag.applyLimit(8);
    var itr = bag.iterator();
    if (!(bag instanceof StructBag)) {
      assertTrue(bag.iterator() instanceof Bag.LimitBagIterator);
    }
    var asList = bag.asList();

    // Check repetition
    var i = 0;
    while (itr.hasNext()) {
      assertEquals(itr.next(), asList.get(i++));
    }
    // Remvove first 5. This means after the completion of iteration the
    // size should be 3
    i = 0;
    itr = bag.iterator();
    while (i < 5) {
      itr.next();
      itr.remove();
      ++i;
    }

    assertEquals(3, bag.size());
    i = 0;
    itr = bag.iterator();
    if (!(bag instanceof StructBag)) {
      assertTrue(bag.iterator() instanceof Bag.LimitBagIterator);
    }
    while (itr.hasNext()) {
      assertEquals(asList.get(i + 5), itr.next());
      ++i;
    }
    assertEquals(i, 3);
  }

  @Test
  public void testValidExceptionThrown() {
    var bag = getBagObject(Integer.class);
    bag.add(wrap(1, bag.getCollectionType().getElementType()));
    bag.add(wrap(2, bag.getCollectionType().getElementType()));
    bag.add(wrap(2, bag.getCollectionType().getElementType()));
    bag.add(wrap(3, bag.getCollectionType().getElementType()));
    bag.add(wrap(3, bag.getCollectionType().getElementType()));
    bag.add(wrap(4, bag.getCollectionType().getElementType()));
    bag.applyLimit(3);
    assertEquals(3, bag.size());

    var itr = bag.iterator();
    try {
      itr.remove();
      fail("should have thrown a IllegalStateException");
    } catch (IllegalStateException e) {
      // pass
    }
    for (var i = 0; i < 3; i++) {
      var n = itr.next();
    }
    try {
      itr.next();
      fail("should have thrown a NoSuchElementException");
    } catch (NoSuchElementException e) {
      // pass
    }

    // test with removes
    itr = bag.iterator();
    for (var i = 0; i < 3; i++) {
      var n = itr.next();
      itr.remove();
    }
    assertEquals(0, bag.size());
    try {
      itr.next();
      fail("should have thrown a NoSuchElementException");
    } catch (NoSuchElementException e) {
      // pass
    }
  }

  @Test
  public void testRemoveAll() {
    var bag = getBagObject(Integer.class);
    // Add Integer & null Objects
    bag.add(wrap(1, bag.getCollectionType().getElementType()));
    bag.add(wrap(2, bag.getCollectionType().getElementType()));
    bag.add(wrap(2, bag.getCollectionType().getElementType()));
    bag.add(wrap(3, bag.getCollectionType().getElementType()));
    bag.add(wrap(3, bag.getCollectionType().getElementType()));
    bag.add(wrap(4, bag.getCollectionType().getElementType()));
    bag.add(wrap(null, bag.getCollectionType().getElementType()));
    bag.add(wrap(null, bag.getCollectionType().getElementType()));
    bag.applyLimit(4);
    var asList = bag.asList();
    asList.add(wrap(13, bag.getCollectionType().getElementType()));
    assertEquals(4, bag.size());
    assertEquals(5, asList.size());
    // Remove all the elements from the list which match the
    // first element pf the list and also
    // Get the number of occurnce of first element of List, in the bag
    var occurrence = bag.occurrences(asList.get(0));
    // Now remove the this element from the list totally
    var toRemove = asList.get(0);
    for (var i = 0; i < occurrence; ++i) {
      asList.remove(toRemove);
    }
    // So we have added one element in thje list which does not exist in the bag
    // and removeed one element from list which exists in the bag.
    bag.removeAll(asList);
    assertEquals(occurrence, bag.size());
    var itr = bag.iterator();
    for (var i = 0; i < occurrence; ++i) {
      itr.next();
    }
    assertFalse(itr.hasNext());
  }

  @Test
  public void testRetainAll() {
    var bag = getBagObject(Integer.class);
    // Add Integer & null Objects
    bag.add(wrap(1, bag.getCollectionType().getElementType()));
    bag.add(wrap(2, bag.getCollectionType().getElementType()));
    bag.add(wrap(2, bag.getCollectionType().getElementType()));
    bag.add(wrap(3, bag.getCollectionType().getElementType()));
    bag.add(wrap(3, bag.getCollectionType().getElementType()));
    bag.add(wrap(4, bag.getCollectionType().getElementType()));
    bag.add(wrap(null, bag.getCollectionType().getElementType()));
    bag.add(wrap(null, bag.getCollectionType().getElementType()));
    bag.applyLimit(4);
    var asList = bag.asList();
    // Asif :Just add an arbit data which is not contained in the bag
    asList.add(wrap(13, bag.getCollectionType().getElementType()));
    bag.retainAll(asList);
    assertEquals(4, bag.size());
    assertEquals(5, asList.size());
    // Remove all the elements from the list which match the
    // first element pf the list and also
    // Get the number of occurnce of first element of List, in the bag
    var occurrence = bag.occurrences(asList.get(0));
    // Now remove the this element from the list totally
    var toRemove = asList.get(0);
    for (var i = 0; i < occurrence; ++i) {
      asList.remove(toRemove);
    }
    // So we have added one element in thje list which does not exist in the bag
    // and removeed one element from list which exists in the bag.
    bag.retainAll(asList);
    assertEquals((4 - occurrence), bag.size());
    var itr = bag.iterator();
    for (var i = 0; i < (4 - occurrence); ++i) {
      itr.next();
    }
    assertFalse(itr.hasNext());
  }

  @Test
  public void testContainAll() {
    var bag = getBagObject(Integer.class);
    // Add Integer & null Objects
    bag.add(wrap(1, bag.getCollectionType().getElementType()));
    bag.add(wrap(2, bag.getCollectionType().getElementType()));
    bag.add(wrap(2, bag.getCollectionType().getElementType()));
    bag.add(wrap(3, bag.getCollectionType().getElementType()));
    bag.add(wrap(3, bag.getCollectionType().getElementType()));
    bag.add(wrap(4, bag.getCollectionType().getElementType()));
    bag.add(wrap(null, bag.getCollectionType().getElementType()));
    bag.add(wrap(null, bag.getCollectionType().getElementType()));
    bag.applyLimit(4);
    var asList = bag.asList();
    // Asif :Just add an arbit data which is not contained in the bag
    // asList.add(wrap(new
    // Integer(13),bag.getCollectionType().getElementType()));
    assertEquals(4, bag.size());
    // assertIndexDetailsEquals(5,asList.size());
    // Remove all the elements from the list which match the
    // first element pf the list
    var occurrence = bag.occurrences(asList.get(0));
    // Now remove the this element from the list totally
    var toRemove = asList.get(0);
    for (var i = 0; i < occurrence; ++i) {
      asList.remove(toRemove);
    }
    assertTrue(bag.containsAll(asList));
    // Asif :Just add an arbit data which is not contained in the bag
    asList.add(wrap(13, bag.getCollectionType().getElementType()));
    assertFalse(bag.containsAll(asList));
  }

  private ResultsBag getBagObject(Class clazz) {
    ObjectType type = new ObjectTypeImpl(clazz);
    return new ResultsBag(type, null);
  }

  private Object wrap(Object obj, ObjectType elementType) {
    return obj;
  }

}
