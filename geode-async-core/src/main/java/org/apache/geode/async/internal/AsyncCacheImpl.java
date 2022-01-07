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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.api.Cache;
import org.apache.geode.async.api.AsyncRegion;

public class AsyncCacheImpl implements AsyncCacheInternal {
  private final Cache cache;
  private final Executor executor;

  private final ConcurrentMap<String, AsyncRegion<?, ?>> regions = new ConcurrentHashMap<>();

  public AsyncCacheImpl(@NotNull final Cache cache, final @NotNull Executor executor) {
    this.cache = cache;
    this.executor = executor;
  }

  @SuppressWarnings("unchecked")
  @Override
  public @NotNull <Key, Value> AsyncRegion<Key, Value> getRegion(@NotNull final String name) {
    return (AsyncRegion<Key, Value>) regions.computeIfAbsent(name,
        (k) -> new AsyncRegionImpl<Key, Value>(this, cache.getRegion(k)));
  }

  @Override
  public @NotNull Executor getExecutor() {
    return executor;
  }
}
