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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.internal.types.ObjectTypeImpl;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.cache.InternalCache;

public class NWayMergeResultsTest {
  private ExecutionContext context;

  @Before
  public void setUp() {
    var mockService = mock(QueryConfigurationService.class);
    when(mockService.getMethodAuthorizer()).thenReturn(mock(MethodInvocationAuthorizer.class));
    var mockCache = mock(InternalCache.class);
    when(mockCache.getService(QueryConfigurationService.class)).thenReturn(mockService);

    context = new ExecutionContext(null, mockCache);
  }

  @Test
  public void testNonDistinct() throws Exception {
    final var numSortedLists = 40;
    Collection<List<Integer>> listOfSortedLists = new ArrayList<>();
    for (var i = 0; i < numSortedLists; ++i) {
      listOfSortedLists.add(new ArrayList<>());
    }
    var step = 0;
    for (var list : listOfSortedLists) {
      step = step + 1;
      for (var i = -500; i < 500; i = i + step) {
        list.add(i);
      }
    }
    var totalElements = 0;
    for (var list : listOfSortedLists) {
      totalElements += list.size();
    }

    var combinedArray = new int[totalElements];
    var i = 0;
    for (var list : listOfSortedLists) {
      for (var num : list) {
        combinedArray[i++] = num;
      }
    }
    Arrays.sort(combinedArray);

    var mergedResults =
        createSingleFieldMergedResult(listOfSortedLists, false, -1);
    var iter = mergedResults.iterator();
    for (var elem : combinedArray) {
      assertThat(iter.next().intValue()).isEqualTo(elem);
    }

    assertThat(iter.hasNext()).isFalse();
    assertThatThrownBy(iter::next)
        .as("next should have thrown NoSuchElementException")
        .isInstanceOf(NoSuchElementException.class);
    assertThat(mergedResults.size()).isEqualTo(combinedArray.length);
  }

  @Test
  public void testDistinct() throws Exception {
    final var numSortedLists = 40;
    Collection<List<Integer>> listOfSortedLists = new ArrayList<>();
    for (var i = 0; i < numSortedLists; ++i) {
      listOfSortedLists.add(new ArrayList<>());
    }
    var step = 0;
    for (var list : listOfSortedLists) {
      step = step + 1;
      for (var i = -500; i < 500; i = i + step) {
        list.add(i);
      }
    }

    SortedSet<Integer> sortedSet = new TreeSet<>();

    for (var list : listOfSortedLists) {
      sortedSet.addAll(list);
    }

    var mergedResults =
        createSingleFieldMergedResult(listOfSortedLists, true, -1);

    var iter = mergedResults.iterator();
    for (int elem : sortedSet) {
      assertThat(iter.next().intValue()).isEqualTo(elem);
    }
    assertThat(iter.hasNext()).isFalse();

    assertThatThrownBy(iter::next)
        .as("next should have thrown NoSuchElementException")
        .isInstanceOf(NoSuchElementException.class);

    assertThat(mergedResults.size()).isEqualTo(sortedSet.size());
  }

  @Test
  public void testLimitNoDistinct() throws Exception {
    final var numSortedLists = 40;
    final var limit = 53;
    Collection<List<Integer>> listOfSortedLists = new ArrayList<>();
    for (var i = 0; i < numSortedLists; ++i) {
      listOfSortedLists.add(new ArrayList<>());
    }
    var step = 0;
    for (var list : listOfSortedLists) {
      step = step + 1;
      for (var i = -500; i < 500; i = i + step) {
        list.add(i);
      }
    }
    var totalElements = 0;
    for (var list : listOfSortedLists) {
      totalElements += list.size();
    }

    var combinedArray = new int[totalElements];
    var i = 0;
    for (var list : listOfSortedLists) {
      for (var num : list) {
        combinedArray[i++] = num;
      }
    }
    Arrays.sort(combinedArray);

    var mergedResults =
        createSingleFieldMergedResult(listOfSortedLists, false, limit);

    var iter = mergedResults.iterator();
    var count = 0;
    for (var elem : combinedArray) {
      if (count == limit) {
        break;
      }
      assertThat(iter.next().intValue()).isEqualTo(elem);
      ++count;
    }

    assertThat(iter.hasNext()).isFalse();
    assertThatThrownBy(iter::next)
        .as("next should have thrown NoSuchElementException")
        .isInstanceOf(NoSuchElementException.class);
    assertThat(mergedResults.size()).isEqualTo(limit);
  }

  @Test
  public void testLimitDistinct() throws Exception {
    final var numSortedLists = 40;
    final var limit = 53;
    Collection<List<Integer>> listOfSortedLists = new ArrayList<>();
    for (var i = 0; i < numSortedLists; ++i) {
      listOfSortedLists.add(new ArrayList<>());
    }
    var step = 0;
    for (var list : listOfSortedLists) {
      step = step + 1;
      for (var i = -500; i < 500; i = i + step) {
        list.add(i);
      }
    }

    SortedSet<Integer> sortedSet = new TreeSet<>();

    for (var list : listOfSortedLists) {
      sortedSet.addAll(list);
    }

    var mergedResults =
        createSingleFieldMergedResult(listOfSortedLists, true, limit);

    var iter = mergedResults.iterator();

    var count = 0;

    for (int elem : sortedSet) {
      if (count == limit) {
        break;
      }
      assertThat(iter.next().intValue()).isEqualTo(elem);
      ++count;
    }
    assertThat(iter.hasNext()).isFalse();
    assertThatThrownBy(iter::next)
        .as("next should have thrown NoSuchElementException")
        .isInstanceOf(NoSuchElementException.class);
    assertThat(mergedResults.size()).isEqualTo(limit);
  }

  @Test
  public void testNonDistinctStruct() throws Exception {
    final var numSortedLists = 40;
    var structType = new StructTypeImpl(new String[] {"a", "b"},
        new ObjectType[] {new ObjectTypeImpl(Integer.TYPE), new ObjectTypeImpl(Integer.TYPE)});
    Collection<List<Struct>> listOfSortedLists = new ArrayList<>();
    for (var i = 0; i < numSortedLists; ++i) {
      listOfSortedLists.add(new ArrayList<>());
    }
    var step = 0;
    for (var list : listOfSortedLists) {
      step = step + 1;
      var j = 1000;
      for (var i = -500; i < 500; i = i + step) {
        Struct struct = new StructImpl(structType,
            new Object[] {i, j - step});
        list.add(struct);
      }
    }
    var totalElements = 0;
    for (var list : listOfSortedLists) {
      totalElements += list.size();
    }

    var combinedArray = new Struct[totalElements];
    var i = 0;
    for (var list : listOfSortedLists) {
      for (var struct : list) {
        combinedArray[i++] = struct;
      }
    }

    Arrays.sort(combinedArray, (o1, o2) -> {
      var fields_1 = o1.getFieldValues();
      var fields_2 = o2.getFieldValues();
      var compare = ((Comparable) fields_1[0]).compareTo(fields_2[0]);
      if (compare == 0) {
        // second field is descending
        compare = ((Comparable) fields_2[1]).compareTo(fields_1[1]);
      }
      return compare;
    });

    var mergedResults =
        createStructFieldMergedResult(listOfSortedLists, structType);

    var iter = mergedResults.iterator();
    for (var elem : combinedArray) {
      assertThat(iter.next()).isEqualTo(elem);
    }
    assertThat(iter.hasNext()).isFalse();
    assertThatThrownBy(iter::next)
        .as("next should have thrown NoSuchElementException")
        .isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void testDistinctStruct() throws Exception {
    final var numSortedLists = 40;
    var structType = new StructTypeImpl(new String[] {"a", "b"},
        new ObjectType[] {new ObjectTypeImpl(Integer.TYPE), new ObjectTypeImpl(Integer.TYPE)});
    Collection<List<Struct>> listOfSortedLists = new ArrayList<>();
    for (var i = 0; i < numSortedLists; ++i) {
      listOfSortedLists.add(new ArrayList<>());
    }
    var step = 0;
    for (var list : listOfSortedLists) {
      step = step + 1;
      var j = 1000;
      for (var i = -500; i < 500; i = i + step) {
        Struct struct = new StructImpl(structType,
            new Object[] {i, j - step});
        list.add(struct);
      }
    }

    @SuppressWarnings("unchecked")
    SortedSet<Struct> sortedSet = new TreeSet<>((o1, o2) -> {
      var fields_1 = o1.getFieldValues();
      var fields_2 = o2.getFieldValues();

      @SuppressWarnings("unchecked")
      var compare = ((Comparable) fields_1[0]).compareTo(fields_2[0]);
      if (compare == 0) {
        // second field is descending
        compare = ((Comparable) fields_2[1]).compareTo(fields_1[1]);
      }
      return compare;
    });

    for (var list : listOfSortedLists) {
      sortedSet.addAll(list);
    }

    var mergedResults =
        createStructFieldMergedResult(listOfSortedLists, structType);

    var iter = mergedResults.iterator();
    for (var elem : sortedSet) {
      assertThat(iter.next()).isEqualTo(elem);
    }
    assertThat(iter.hasNext()).isFalse();
    assertThatThrownBy(iter::next)
        .as("next should have thrown NoSuchElementException")
        .isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void testOccurrenceNonDistinct() throws Exception {
    final var numSortedLists = 40;
    Collection<List<Integer>> listOfSortedLists = new ArrayList<>();
    for (var i = 0; i < numSortedLists; ++i) {
      listOfSortedLists.add(new ArrayList<>());
    }
    var step = 0;
    for (var list : listOfSortedLists) {
      step = step + 1;
      for (var i = -500; i < 500; i = i + step) {
        list.add(i);
      }
    }
    var totalElements = 0;
    for (var list : listOfSortedLists) {
      totalElements += list.size();
    }

    var combinedArray = new int[totalElements];
    var i = 0;
    for (var list : listOfSortedLists) {
      for (var num : list) {
        combinedArray[i++] = num;
      }
    }
    Arrays.sort(combinedArray);
    // count occurrence of 70, 72,73 , 75
    int num70 = 0, num72 = 0, num73 = 0, num75 = 0;
    for (var num : combinedArray) {
      if (num == 70) {
        ++num70;
      } else if (num == 72) {
        ++num72;
      } else if (num == 73) {
        ++num73;
      } else if (num == 75) {
        ++num75;
      }
    }
    var mergedResults =
        createSingleFieldMergedResult(listOfSortedLists, false, -1);
    assertThat(mergedResults.occurrences(70)).isEqualTo(num70);
    assertThat(mergedResults.occurrences(72)).isEqualTo(num72);
    assertThat(mergedResults.occurrences(73)).isEqualTo(num73);
    assertThat(mergedResults.occurrences(75)).isEqualTo(num75);
  }

  @Test
  public void testOccurrenceDistinct() throws Exception {
    final var numSortedLists = 40;
    Collection<List<Integer>> listOfSortedLists = new ArrayList<>();
    for (var i = 0; i < numSortedLists; ++i) {
      listOfSortedLists.add(new ArrayList<>());
    }
    var step = 0;
    for (var list : listOfSortedLists) {
      step = step + 1;
      for (var i = -500; i < 500; i = i + step) {
        list.add(i);
      }
    }

    var mergedResults =
        createSingleFieldMergedResult(listOfSortedLists, true, -1);

    assertThat(mergedResults.occurrences(70)).isEqualTo(1);
    assertThat(mergedResults.occurrences(72)).isEqualTo(1);
    assertThat(mergedResults.occurrences(73)).isEqualTo(1);
    assertThat(mergedResults.occurrences(75)).isEqualTo(1);
  }

  private <E> NWayMergeResults<E> createSingleFieldMergedResult(
      Collection<? extends Collection<E>> sortedResults, boolean isDistinct, int limit)
      throws Exception {
    var csc = new CompiledSortCriterion(false,
        CompiledSortCriterion.ProjectionField.getProjectionField());
    var method = CompiledSortCriterion.class
        .getDeclaredMethod("substituteExpressionWithProjectionField", Integer.TYPE);
    method.setAccessible(true);
    method.invoke(csc, 0);
    List<CompiledSortCriterion> orderByAttribs = new ArrayList<>();
    orderByAttribs.add(csc);
    ObjectType elementType = new ObjectTypeImpl(Object.class);

    return new NWayMergeResults<>(sortedResults, isDistinct, limit, orderByAttribs, context,
        elementType);
  }

  private NWayMergeResults<Struct> createStructFieldMergedResult(
      Collection<? extends Collection<Struct>> sortedResults,
      StructTypeImpl structType) throws Exception {
    var csc1 = new CompiledSortCriterion(false,
        CompiledSortCriterion.ProjectionField.getProjectionField());
    var csc2 =
        new CompiledSortCriterion(true, CompiledSortCriterion.ProjectionField.getProjectionField());
    var method = CompiledSortCriterion.class
        .getDeclaredMethod("substituteExpressionWithProjectionField", Integer.TYPE);
    method.setAccessible(true);
    method.invoke(csc1, 0);
    method.invoke(csc2, 1);
    List<CompiledSortCriterion> orderByAttribs = new ArrayList<>();
    orderByAttribs.add(csc1);
    orderByAttribs.add(csc2);

    return new NWayMergeResults<>(sortedResults, false, -1, orderByAttribs, context,
        structType);
  }

  @Test
  public void testCombination() throws Exception {
    List<String> results1 = new ArrayList<>();
    results1.add("IBM");
    results1.add("YHOO");

    List<String> results2 = new ArrayList<>();
    results2.add("APPL");
    results2.add("ORCL");

    List<String> results3 = new ArrayList<>();
    results3.add("DELL");
    results3.add("RHAT");

    List<String> results4 = new ArrayList<>();
    results4.add("ORCL");
    results4.add("SAP");

    List<String> results5 = Collections.emptyList();
    List<String> results6 = Collections.emptyList();
    List<String> results7 = Collections.emptyList();

    List<Collection<String>> sortedLists = new ArrayList<>();
    sortedLists.add(results1);
    sortedLists.add(results2);
    sortedLists.add(results3);
    sortedLists.add(results4);
    sortedLists.add(results5);
    sortedLists.add(results6);
    sortedLists.add(results7);

    var mergedResults = createSingleFieldMergedResult(sortedLists, true, -1);

    List<String> results8 = new ArrayList<>();
    results8.add("DELL");
    results8.add("GOOG");
    results8.add("HP");
    results8.add("MSFT");
    results8.add("SAP");
    results8.add("SUN");

    List<String> results9 = new ArrayList<>();
    results9.add("AOL");
    results9.add("APPL");
    results9.add("GOOG");
    results9.add("IBM");
    results9.add("SUN");
    results9.add("YHOO");

    List<Collection<String>> sortedLists1 = new ArrayList<>();
    sortedLists1.add(mergedResults);
    sortedLists1.add(results8);
    sortedLists1.add(results9);

    var netMergedResults =
        createSingleFieldMergedResult(sortedLists1, true, -1);

    assertThat(netMergedResults.size()).isEqualTo(12);
  }

}
