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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.Executor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.geode.api.Cache;
import org.apache.geode.api.Region;

class AsyncCacheImplTest {
  final Executor executor = mock(Executor.class);
  final Cache syncCache = mock(Cache.class);
  @SuppressWarnings("unchecked")
  final Region<String, String> syncRegion = mock(Region.class);
  final AsyncCacheImpl cache = new AsyncCacheImpl(syncCache, executor);

  @BeforeEach
  public void beforeEach() {
    when(syncCache.<String, String>getRegion(eq("region"))).thenReturn(syncRegion);
  }

  @Test
  public void getExecutorNotNull() {
    assertThat(cache.getExecutor()).isSameAs(executor);
  }

  @Test
  public void getRegionExistsReturnsRegion() {
    assertThat(cache.getRegion("region")).isNotNull();
  }

  @Test
  public void getRegionReturnsSameRegion() {
    assertThat(cache.getRegion("region")).isSameAs(cache.getRegion("region")).isNotNull();
  }

}
