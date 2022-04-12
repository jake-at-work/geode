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

package org.apache.geode.internal.lang.utils;

import java.util.Map;
import java.util.function.Function;

public class JavaWorkarounds {

  /**
   * This is a workaround for computeIfAbsent which unnecessarily takes out a write lock in the case
   * where the entry for the key exists already. This only affects pre Java 9 jdks
   * https://bugs.openjdk.java.net/browse/JDK-8161372
   *
   * @see Map#computeIfAbsent(Object, Function)
   */
  public static <K, V> V computeIfAbsent(Map<K, V> map, K key,
      Function<? super K, ? extends V> mappingFunction) {
    var existingValue = map.get(key);
    if (existingValue == null) {
      return map.computeIfAbsent(key, mappingFunction);
    }
    return existingValue;
  }
}
