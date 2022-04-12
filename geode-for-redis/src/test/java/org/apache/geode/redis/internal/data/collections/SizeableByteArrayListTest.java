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
package org.apache.geode.redis.internal.data.collections;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.internal.size.ReflectionObjectSizer;

public class SizeableByteArrayListTest {
  private final ObjectSizer sizer = ReflectionObjectSizer.getInstance();
  private final int INITIAL_NUMBER_OF_ELEMENTS = 20;

  @Test
  public void getSizeInBytesIsAccurate_ForEmptySizeableByteArrayList() {
    var list = new SizeableByteArrayList();
    assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));
  }

  @Test
  public void getSizeInBytesIsAccurate_ForSizeableByteArrayListElements() {
    var elementsToAdd = 100;

    // Create a list with an initial size and confirm that it correctly reports its size
    var list = createList();
    assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));

    // Add elements and assert that the size is correct after each add
    for (var i = INITIAL_NUMBER_OF_ELEMENTS; i < INITIAL_NUMBER_OF_ELEMENTS + elementsToAdd; ++i) {
      list.addFirst(makeByteArrayOfSpecifiedLength(i));
      assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));
    }
    assertThat(list.size()).isEqualTo(INITIAL_NUMBER_OF_ELEMENTS + elementsToAdd);

    // Remove all the elements and assert that the size is correct after each remove
    for (var i = 0; i < INITIAL_NUMBER_OF_ELEMENTS + elementsToAdd; ++i) {
      list.remove(0);
      assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));
    }
    assertThat(list.size()).isEqualTo(0);
  }

  @Test
  public void removeObjects_getSizeInBytesIsAccurate() {
    // Create a list with an initial size and confirm that it correctly reports its size
    var list = createList();
    assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));

    // Remove all the elements and assert that the size is correct after each remove
    var rand = new Random();
    for (var i = 0; i < INITIAL_NUMBER_OF_ELEMENTS; ++i) {
      list.remove(makeByteArrayOfSpecifiedLength(i + 1), rand.nextInt(3) - 1);
      assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));
    }
    assertThat(list.size()).isEqualTo(0);
  }

  @Test
  public void removeIndexes_getSizeInBytesIsAccurate() {
    // Create a list with an initial size and confirm that it correctly reports its size
    var list = createList();
    assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));

    // Remove all the elements and assert that the size is correct after each remove
    for (var i = INITIAL_NUMBER_OF_ELEMENTS - 1; 0 <= i; --i) {
      List<Integer> indexToRemove = new ArrayList<>(1);
      indexToRemove.add(i);
      list.removeIndexes(indexToRemove);
      assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));
    }
    assertThat(list.size()).isEqualTo(0);
  }

  private SizeableByteArrayList createList() {
    var list = new SizeableByteArrayList();
    for (var i = 0; i < INITIAL_NUMBER_OF_ELEMENTS; ++i) {
      list.addFirst(makeByteArrayOfSpecifiedLength(i + 1));
    }
    return list;
  }

  private byte[] makeByteArrayOfSpecifiedLength(int length) {
    var newByteArray = new byte[length];
    for (var i = 0; i < length; i++) {
      newByteArray[i] = (byte) i;
    }
    return newByteArray;
  }

}
