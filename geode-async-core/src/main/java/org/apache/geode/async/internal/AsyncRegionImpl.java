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

import static java.util.Objects.requireNonNull;

import java.util.concurrent.CompletableFuture;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.api.EntryFunction;
import org.apache.geode.api.Region;
import org.apache.geode.async.api.AsyncRegion;

public class AsyncRegionImpl<Key, Value> implements AsyncRegion<Key, Value> {

  private final Region<Key, Value> region;
  private final AsyncCacheInternal cache;

  public AsyncRegionImpl(final @NotNull AsyncCacheInternal cache,
      final @NotNull Region<Key, Value> region) {
    this.cache = cache;
    this.region = region;
  }

  @Override
  public @NotNull <Result> CompletableFuture<Result> execute(@NotNull final Key key,
      @NotNull final EntryFunction<Key, Value, Result> function) {
    requireNonNull(key);
    requireNonNull(function);

    return CompletableFuture.supplyAsync(() -> region.execute(key, function), cache.getExecutor());
  }

}
