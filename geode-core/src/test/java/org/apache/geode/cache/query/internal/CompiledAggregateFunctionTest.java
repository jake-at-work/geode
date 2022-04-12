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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.query.Aggregator;
import org.apache.geode.cache.query.internal.aggregate.Avg;
import org.apache.geode.cache.query.internal.aggregate.AvgBucketNode;
import org.apache.geode.cache.query.internal.aggregate.AvgDistinct;
import org.apache.geode.cache.query.internal.aggregate.AvgDistinctPRQueryNode;
import org.apache.geode.cache.query.internal.aggregate.AvgPRQueryNode;
import org.apache.geode.cache.query.internal.aggregate.Count;
import org.apache.geode.cache.query.internal.aggregate.CountDistinct;
import org.apache.geode.cache.query.internal.aggregate.CountDistinctPRQueryNode;
import org.apache.geode.cache.query.internal.aggregate.CountPRQueryNode;
import org.apache.geode.cache.query.internal.aggregate.DistinctAggregator;
import org.apache.geode.cache.query.internal.aggregate.MaxMin;
import org.apache.geode.cache.query.internal.aggregate.Sum;
import org.apache.geode.cache.query.internal.aggregate.SumDistinct;
import org.apache.geode.cache.query.internal.aggregate.SumDistinctPRQueryNode;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.internal.cache.InternalCache;

public class CompiledAggregateFunctionTest {
  private InternalCache cache;
  private List<Integer> bucketList;

  @Before
  public void setUp() throws Exception {
    cache = mock(InternalCache.class);
    var mockService = mock(QueryConfigurationService.class);
    when(mockService.getMethodAuthorizer()).thenReturn(mock(MethodInvocationAuthorizer.class));
    when(cache.getService(QueryConfigurationService.class)).thenReturn(mockService);

    bucketList = Collections.singletonList(1);
  }

  @Test
  public void testCount() throws Exception {
    var caf1 = new CompiledAggregateFunction(null, OQLLexerTokenTypes.COUNT);
    var context1 = new ExecutionContext(null, cache);
    assertThat(caf1.evaluate(context1)).isInstanceOf(Count.class);

    var caf2 =
        new CompiledAggregateFunction(null, OQLLexerTokenTypes.COUNT, true);
    var context2 = new ExecutionContext(null, cache);
    assertThat(caf2.evaluate(context2)).isInstanceOf(CountDistinct.class);

    var caf3 = new CompiledAggregateFunction(null, OQLLexerTokenTypes.COUNT);
    var context3 = new ExecutionContext(null, cache);
    context3.setIsPRQueryNode(true);
    assertThat(caf3.evaluate(context3)).isInstanceOf(CountPRQueryNode.class);

    var caf4 = new CompiledAggregateFunction(null, OQLLexerTokenTypes.COUNT);
    var context4 = new QueryExecutionContext(null, cache);

    context4.setBucketList(bucketList);
    assertThat(caf4.evaluate(context4)).isInstanceOf(Count.class);

    var caf5 =
        new CompiledAggregateFunction(null, OQLLexerTokenTypes.COUNT, true);
    var context5 = new ExecutionContext(null, cache);
    context5.setIsPRQueryNode(true);
    assertThat(caf5.evaluate(context5)).isInstanceOf(CountDistinctPRQueryNode.class);

    var caf6 =
        new CompiledAggregateFunction(null, OQLLexerTokenTypes.COUNT, true);
    var context6 = new QueryExecutionContext(null, cache);
    context6.setBucketList(bucketList);
    assertThat(caf6.evaluate(context6)).isInstanceOf(DistinctAggregator.class);
  }

  @Test
  public void testSum() throws Exception {
    var caf1 = new CompiledAggregateFunction(null, OQLLexerTokenTypes.SUM);
    var context1 = new ExecutionContext(null, cache);
    assertThat(caf1.evaluate(context1)).isInstanceOf(Sum.class);

    var caf2 =
        new CompiledAggregateFunction(null, OQLLexerTokenTypes.SUM, true);
    var context2 = new ExecutionContext(null, cache);
    assertThat(caf2.evaluate(context2)).isInstanceOf(SumDistinct.class);

    var caf3 = new CompiledAggregateFunction(null, OQLLexerTokenTypes.SUM);
    var context3 = new ExecutionContext(null, cache);
    context3.setIsPRQueryNode(true);
    assertThat(caf3.evaluate(context3)).isInstanceOf(Sum.class);

    var caf4 = new CompiledAggregateFunction(null, OQLLexerTokenTypes.SUM);
    var context4 = new QueryExecutionContext(null, cache);
    context4.setBucketList(bucketList);
    assertThat(caf4.evaluate(context4)).isInstanceOf(Sum.class);

    var caf5 =
        new CompiledAggregateFunction(null, OQLLexerTokenTypes.SUM, true);
    var context5 = new ExecutionContext(null, cache);
    context5.setIsPRQueryNode(true);
    assertThat(caf5.evaluate(context5)).isInstanceOf(SumDistinctPRQueryNode.class);

    var caf6 =
        new CompiledAggregateFunction(null, OQLLexerTokenTypes.SUM, true);
    var context6 = new QueryExecutionContext(null, cache);
    context6.setBucketList(bucketList);
    assertThat(caf6.evaluate(context6)).isInstanceOf(DistinctAggregator.class);
  }

  @Test
  public void testAvg() throws Exception {
    var caf1 = new CompiledAggregateFunction(null, OQLLexerTokenTypes.AVG);
    var context1 = new ExecutionContext(null, cache);
    assertThat(caf1.evaluate(context1)).isInstanceOf(Avg.class);

    var caf2 =
        new CompiledAggregateFunction(null, OQLLexerTokenTypes.AVG, true);
    var context2 = new ExecutionContext(null, cache);
    assertThat(caf2.evaluate(context2)).isInstanceOf(AvgDistinct.class);

    var caf3 = new CompiledAggregateFunction(null, OQLLexerTokenTypes.AVG);
    var context3 = new ExecutionContext(null, cache);
    context3.setIsPRQueryNode(true);
    assertThat(caf3.evaluate(context3)).isInstanceOf(AvgPRQueryNode.class);

    var caf4 = new CompiledAggregateFunction(null, OQLLexerTokenTypes.AVG);
    var context4 = new QueryExecutionContext(null, cache);
    context4.setBucketList(bucketList);
    assertThat(caf4.evaluate(context4)).isInstanceOf(AvgBucketNode.class);

    var caf5 =
        new CompiledAggregateFunction(null, OQLLexerTokenTypes.AVG, true);
    var context5 = new ExecutionContext(null, cache);
    context5.setIsPRQueryNode(true);
    assertThat(caf5.evaluate(context5)).isInstanceOf(AvgDistinctPRQueryNode.class);

    var caf6 =
        new CompiledAggregateFunction(null, OQLLexerTokenTypes.AVG, true);
    var context6 = new QueryExecutionContext(null, cache);
    context6.setBucketList(bucketList);
    assertThat(caf6.evaluate(context6)).isInstanceOf(DistinctAggregator.class);
  }

  @Test
  public void testMaxMin() throws Exception {
    var caf1 = new CompiledAggregateFunction(null, OQLLexerTokenTypes.MAX);
    var context1 = new ExecutionContext(null, cache);
    var agg = (Aggregator) caf1.evaluate(context1);
    assertThat(agg).isInstanceOf(MaxMin.class);
    var maxMin = (MaxMin) agg;
    Class maxMinClass = MaxMin.class;
    var findMax = maxMinClass.getDeclaredField("findMax");
    findMax.setAccessible(true);
    assertThat(findMax.get(maxMin)).isEqualTo(Boolean.TRUE);

    var caf2 = new CompiledAggregateFunction(null, OQLLexerTokenTypes.MIN);
    var agg1 = (Aggregator) caf2.evaluate(context1);
    assertThat(agg1).isInstanceOf(MaxMin.class);
    var maxMin1 = (MaxMin) agg1;
    assertThat(findMax.get(maxMin1)).isEqualTo(Boolean.FALSE);
  }
}
