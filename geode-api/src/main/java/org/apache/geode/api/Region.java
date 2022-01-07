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

package org.apache.geode.api;

import java.util.Map;
import java.util.Objects;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface Region<Key, Value> {
  <Result> Result execute(@NotNull Key key, @NotNull EntryFunction<Key, Value, Result> function);

  default @Nullable Value getValue(@NotNull Key key) {
    return execute(key, Map.Entry::getValue);
  }

  default void setValue(@NotNull Key key, @NotNull Value value) {
    execute(key, entry -> {
      entry.setValue(value);
      return null;
    });
  }

  default @Nullable Value getAndSetValue(@NotNull Key key, @NotNull Value value) {
    return execute(key, entry -> entry.setValue(value));
  }

  default void removeValue(@NotNull Key key) {
    execute(key, entry -> {
      entry.setValue(null);
      return null;
    });
  }

  default @Nullable Value getAndRemoveValue(@NotNull Key key) {
    return execute(key, entry -> entry.setValue(null));
  }

  default boolean tryRemoveValue(@NotNull Key key) {
    return execute(key, entry -> null != entry.setValue(null));
  }

  default void replaceValue(@NotNull Key key, @NotNull Value expectedValue,
      @NotNull Value newValue) {
    execute(key, entry -> {
      if (Objects.equals(entry.getValue(), expectedValue)) {
        entry.setValue(newValue);
      }
      return null;
    });
  }

  default @Nullable Value getAndReplaceValue(@NotNull Key key, @NotNull Value expectedValue,
      @NotNull Value newValue) {
    return execute(key, entry -> {
      if (Objects.equals(entry.getValue(), expectedValue)) {
        return entry.setValue(newValue);
      }
      return entry.getValue();
    });
  }

  default boolean tryReplaceValue(@NotNull Key key, @NotNull Value expectedValue,
      @NotNull Value newValue) {
    return execute(key, entry -> {
      if (Objects.equals(entry.getValue(), expectedValue)) {
        entry.setValue(newValue);
        return true;
      }
      return false;
    });
  }

}
