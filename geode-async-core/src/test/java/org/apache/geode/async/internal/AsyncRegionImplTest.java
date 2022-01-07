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

package org.apache.geode.async.internal;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.geode.api.EntryFunction;
import org.apache.geode.api.Region;

class AsyncRegionImplTest {

  final AsyncCacheInternal cache = mock(AsyncCacheInternal.class);

  @SuppressWarnings("unchecked")
  final Region<String, String> syncRegion = mock(Region.class);

  final AsyncRegionImpl<String, String> region = new AsyncRegionImpl<>(cache, syncRegion);

  @BeforeEach
  public void beforeEach() {
    @SuppressWarnings("unchecked")
    final Map.Entry<String, String> mockEntry = mock(Map.Entry.class);
    when(mockEntry.getKey()).thenReturn("key");
    when(mockEntry.getValue()).thenReturn("value");
    when(syncRegion.execute(eq("key"), any())).thenAnswer(
        (i) -> i.<EntryFunction<String, String, String>>getArgument(1).execute(mockEntry));
    when(cache.getExecutor()).thenReturn(ForkJoinPool.commonPool());
  }

  @Test
  public void executeReturnsFuture() throws ExecutionException, InterruptedException {
    final CompletableFuture<String> future = region.execute("key", entry -> {
      assertThat(entry.getKey()).isEqualTo("key");
      assertThat(entry.getValue()).isEqualTo("value");
      return entry.getValue();
    });

    assertThat(future).isNotNull();
    await().until(future::isDone);
    assertThat(future.get()).isEqualTo("value");
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void executeThrowsOnNullKey() {
    assertThatThrownBy(() -> region.execute(null, entry -> null))
        .isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void executeThrowsOnNullFunction() {
    assertThatThrownBy(() -> region.execute("key", null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void executeReturnsCompletableFuture() throws ExecutionException, InterruptedException {
    final CompletableFuture<String> future =
        region.execute("key", Map.Entry::getValue).thenApply(s -> s + "-foo");

    await().until(future::isDone);
    assertThat(future.get()).isEqualTo("value-foo");
  }

}
