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

import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.api.Cache;
import org.apache.geode.async.api.AsyncCache;
import org.apache.geode.async.api.AsyncCacheBuilder;

public class AsyncCacheBuilderFromCache implements AsyncCacheBuilder {
  private final Cache cache;
  private Executor executor;

  public AsyncCacheBuilderFromCache(final Cache cache, final Executor executor) {
    this.cache = cache;
    this.executor = executor;
  }

  @Override
  public AsyncCache build() {
    return new AsyncCacheImpl(cache, null == executor ? ForkJoinPool.commonPool() : executor);
  }

  @Override
  public AsyncCacheBuilder fromCache(final @NotNull Cache cache) {
    throw new IllegalStateException("Already invoked.");
  }

  @Override
  public AsyncCacheBuilder withExecutor(@NotNull final Executor executor) {
    this.executor = executor;
    return this;
  }
}
