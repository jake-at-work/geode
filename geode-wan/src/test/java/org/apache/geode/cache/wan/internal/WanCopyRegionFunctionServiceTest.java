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
package org.apache.geode.cache.wan.internal;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class WanCopyRegionFunctionServiceTest {

  private WanCopyRegionFunctionService service;
  private final InternalCache cache = mock(InternalCache.class);

  @Before
  public void setUp() throws Exception {
    service = new WanCopyRegionFunctionService();
    service.init(cache);
  }

  @Test
  public void severalExecuteWithSameRegionAndSenderNotAllowed() {
    var latch = new CountDownLatch(1);
    var firstExecution = (Callable<CliFunctionResult>) () -> {
      latch.await(GeodeAwaitility.getTimeout().getSeconds(), TimeUnit.SECONDS);
      return null;
    };

    var regionName = "myRegion";
    var senderId = "mySender";
    CompletableFuture
        .supplyAsync(() -> {
          try {
            return service.execute(firstExecution, regionName, senderId);
          } catch (Exception e) {
            return null;
          }
        });

    // Wait for the execute function to start
    await().untilAsserted(() -> assertThat(service.getNumberOfCurrentExecutions()).isEqualTo(1));

    // Execute another function instance for the same region and sender-id
    var secondExecution = (Callable<CliFunctionResult>) () -> null;

    assertThatThrownBy(() -> service.execute(secondExecution, regionName, senderId))
        .isInstanceOf(WanCopyRegionFunctionServiceAlreadyRunningException.class);

    // Let first execution finish
    latch.countDown();
  }

  @Test
  public void cancelRunningExecutionReturnsSuccess() {
    var regionName = "myRegion";
    var senderId = "mySender";
    var latch = new CountDownLatch(1);
    var firstExecution = (Callable<CliFunctionResult>) () -> {
      latch.await(GeodeAwaitility.getTimeout().getSeconds(), TimeUnit.SECONDS);
      return null;
    };
    CompletableFuture
        .supplyAsync(() -> {
          try {
            return service.execute(firstExecution, regionName, senderId);
          } catch (Exception e) {
            return null;
          }
        });

    // Wait for the function to start execution
    await().untilAsserted(() -> assertThat(service.getNumberOfCurrentExecutions()).isEqualTo(1));

    // Cancel the function execution
    var result = service.cancel(regionName, senderId);

    assertThat(result).isEqualTo(true);
    await().untilAsserted(() -> assertThat(service.getNumberOfCurrentExecutions()).isEqualTo(0));
  }

  @Test
  public void cancelNotRunningExecutionReturnsError() {
    final var regionName = "myRegion";
    final var senderId = "mySender";

    var result = service.cancel(regionName, senderId);

    assertThat(result).isEqualTo(false);
  }

  @Test
  public void cancelAllExecutionsWithRunningExecutionsReturnsCanceledExecutions() {
    var latch = new CountDownLatch(2);
    var firstExecution = (Callable<CliFunctionResult>) () -> {
      latch.await(GeodeAwaitility.getTimeout().getSeconds(), TimeUnit.SECONDS);
      return null;
    };

    CompletableFuture
        .supplyAsync(() -> {
          try {
            return service.execute(firstExecution, "myRegion", "mySender1");
          } catch (Exception e) {
            return null;
          }
        });

    var secondExecution = (Callable<CliFunctionResult>) () -> {
      latch.await(GeodeAwaitility.getTimeout().getSeconds(), TimeUnit.SECONDS);
      return null;
    };

    CompletableFuture
        .supplyAsync(() -> {
          try {
            return service.execute(secondExecution, "myRegion", "mySender");
          } catch (Exception e) {
            return null;
          }
        });

    // Wait for the functions to start execution
    await().untilAsserted(() -> assertThat(service.getNumberOfCurrentExecutions()).isEqualTo(2));

    // Cancel the function execution
    var executionsString = service.cancelAll();

    assertThat(executionsString).isEqualTo("[(myRegion,mySender1), (myRegion,mySender)]");
    await().untilAsserted(() -> assertThat(service.getNumberOfCurrentExecutions()).isEqualTo(0));
  }

  @Test
  public void severalExecuteWithDifferentRegionOrSenderAreAllowed() {
    var executions = 5;
    var latch = new CountDownLatch(executions);
    for (var i = 0; i < executions; i++) {
      var firstExecution = (Callable<CliFunctionResult>) () -> {
        latch.await(GeodeAwaitility.getTimeout().getSeconds(), TimeUnit.SECONDS);
        return null;
      };

      final var regionName = String.valueOf(i);
      CompletableFuture
          .supplyAsync(() -> {
            try {
              return service.execute(firstExecution, regionName, "mySender1");
            } catch (Exception e) {
              return null;
            }
          });
    }

    // Wait for the functions to start execution
    await().untilAsserted(
        () -> assertThat(service.getNumberOfCurrentExecutions()).isEqualTo(executions));

    // End executions
    for (var i = 0; i < executions; i++) {
      latch.countDown();
    }
  }
}
