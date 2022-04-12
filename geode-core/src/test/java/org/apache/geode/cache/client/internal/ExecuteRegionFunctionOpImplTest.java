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
package org.apache.geode.cache.client.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.client.internal.ExecuteRegionFunctionOp.ExecuteRegionFunctionOpImpl;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.internal.cache.execute.ServerRegionFunctionExecutor;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;


/**
 * Test ExecutionRegionFunctionOpImpl class
 */
@Category({ClientServerTest.class})
@RunWith(GeodeParamsRunner.class)
public class ExecuteRegionFunctionOpImplTest {

  @Test
  public void testExecuteRegionFunctionOpImplWithFunction() {
    var op = createOpWithFunctionTwoFilters();

    var numberOfParts = 10;
    assertEquals(numberOfParts, op.getMessage().getNumberOfParts());
    for (var i = 0; i < numberOfParts; i++) {
      assertNotNull(op.getMessage().getPart(i));
    }
    assertNull(op.getMessage().getPart(numberOfParts));
  }

  @Test
  public void testExecuteRegionFunctionOpImplWithFunctionIdCalculateFnState() {
    var op = createOpWithFunctionIdOneFilter();

    var numberOfParts = 9;
    assertEquals(numberOfParts, op.getMessage().getNumberOfParts());
    for (var i = 0; i < numberOfParts; i++) {
      assertNotNull(op.getMessage().getPart(i));
    }
    assertNull(op.getMessage().getPart(numberOfParts));
  }

  @Test
  public void testExecuteRegionFunctionOpImplWithOpAndIsReexecute() {
    var op = createOpWithFunctionTwoFilters();

    HashSet<String> removedNodes = new HashSet(Arrays.asList("node1", "node2", "node3"));

    var newOp =
        new ExecuteRegionFunctionOp.ExecuteRegionFunctionOpImpl(op, (byte) 1, removedNodes);

    var numberOfParts = 13;
    assertEquals(numberOfParts, newOp.getMessage().getNumberOfParts());
    for (var i = 0; i < numberOfParts; i++) {
      assertNotNull(newOp.getMessage().getPart(i));
    }
    assertNull(newOp.getMessage().getPart(numberOfParts));
  }

  private ExecuteRegionFunctionOp.ExecuteRegionFunctionOpImpl createOpWithFunctionTwoFilters() {
    var region = "testRegion";
    var functionId = "testFunctionId";
    var function = mock(Function.class);
    var serverRegionExecutor = mock(ServerRegionFunctionExecutor.class);
    Set filter = new HashSet(Arrays.asList("one", "two"));
    when(serverRegionExecutor.getFilter()).thenReturn(filter);
    var functionState = (byte) 1;
    var flags = (byte) 2;
    var resultCollector = mock(ResultCollector.class);
    var timeoutMs = 100;

    var op =
        new ExecuteRegionFunctionOp.ExecuteRegionFunctionOpImpl(region, function,
            serverRegionExecutor, resultCollector,
            timeoutMs);
    return op;
  }

  private ExecuteRegionFunctionOpImpl createOpWithFunctionIdOneFilter() {
    var region = "testRegion";
    var functionId = "testFunctionId";
    Function function = null;
    var serverRegionExecutor = mock(ServerRegionFunctionExecutor.class);
    Set filter = new HashSet(Arrays.asList("one"));
    when(serverRegionExecutor.getFilter()).thenReturn(filter);
    var functionState = (byte) 1;
    var flags = (byte) 1;
    var hasResult = (byte) 1;
    var isHA = false;
    var resultCollector = mock(ResultCollector.class);
    var timeoutMs = 100;
    var optimizeForWrite = true;
    var isReexecute = false;

    var op = new ExecuteRegionFunctionOpImpl(region, functionId,
        serverRegionExecutor, resultCollector, hasResult, isHA, optimizeForWrite,
        true, timeoutMs);
    return op;
  }

}
