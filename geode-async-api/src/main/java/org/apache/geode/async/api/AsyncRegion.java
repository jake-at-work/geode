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

package org.apache.geode.async.api;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.geode.api.EntryFunction;

public interface AsyncRegion<Key, Value> {

  @NotNull
  <Result> CompletableFuture<Result> execute(@NotNull Key key,
      @NotNull EntryFunction<Key, Value, Result> function);

  default @NotNull CompletableFuture<@Nullable Value> getValue(@NotNull Key key) {
    return execute(key, Map.Entry::getValue);
  }

  default @NotNull CompletableFuture<Void> setValue(@NotNull Key key, @NotNull Value value) {
    return execute(key, entry -> {
      entry.setValue(value);
      return null;
    });
  }

  default @NotNull CompletableFuture<Value> getAndSetValue(@NotNull Key key, @NotNull Value value) {
    return execute(key, entry -> entry.setValue(value));
  }

  default @NotNull CompletableFuture<Void> removeValue(@NotNull Key key) {
    return execute(key, entry -> {
      entry.setValue(null);
      return null;
    });
  }

  default @NotNull CompletableFuture<Value> getAndRemoveValue(@NotNull Key key) {
    return execute(key, entry -> entry.setValue(null));
  }

  default @NotNull CompletableFuture<Boolean> tryRemoveValue(@NotNull Key key) {
    return execute(key, entry -> null != entry.setValue(null));
  }

  default @NotNull CompletableFuture<Value> getAndReplaceValue(@NotNull Key key,
      @NotNull Value expectedValue, @NotNull Value newValue) {
    return execute(key, entry -> {
      if (Objects.equals(entry.getValue(), expectedValue)) {
        return entry.setValue(newValue);
      }
      return entry.getValue();
    });
  }

  default @NotNull CompletableFuture<Boolean> tryReplaceValue(@NotNull Key key,
      @NotNull Value expectedValue, @NotNull Value newValue) {
    return execute(key, entry -> {
      if (Objects.equals(entry.getValue(), expectedValue)) {
        entry.setValue(newValue);
        return true;
      }
      return false;
    });
  }

  default @NotNull CompletableFuture<Void> replaceValue(@NotNull Key key,
      @NotNull Value expectedValue, @NotNull Value newValue) {
    return execute(key, entry -> {
      if (Objects.equals(entry.getValue(), expectedValue)) {
        entry.setValue(newValue);
      }
      return null;
    });
  }

}
