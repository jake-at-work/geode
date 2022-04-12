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
package org.apache.geode.cache.query.cq.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class ServerCQResultsCacheReplicateRegionImplTest {
  @Rule
  public ExecutorServiceRule executorService = new ExecutorServiceRule();

  private final ServerCQResultsCacheReplicateRegionImpl serverCQResultCache =
      new ServerCQResultsCacheReplicateRegionImpl();
  private final ArrayList<String> objects = new ArrayList<>();
  private final Random random = new Random(1188999);
  private String targetKey;
  private boolean isDone = false;

  @Before
  public void setup() {
    var index = 122;
    var prefix = "object_";
    targetKey = prefix + index;
    var size = 1000;
    for (var i = 0; i < size; i++) {
      if (i != index) {
        objects.add(prefix + i);
      }
    }
  }

  @Test
  public void serverCQResultCacheContainsReturnsCorrectResult() throws Exception {
    serverCQResultCache.setInitialized();
    serverCQResultCache.add(targetKey);

    var future = executorService.submit(this::verifyContains);

    var numberThreads = 10;
    var numberOfOperations = 1000;
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (var i = 0; i < numberThreads; i++) {
      futures.add(executorService.runAsync(() -> doOperations(numberOfOperations)));
    }

    for (var completableFuture : futures) {
      completableFuture.join();
    }

    isDone = true;
    future.get();
  }

  private void verifyContains() {
    while (!isDone) {
      assertThat(serverCQResultCache.contains(targetKey)).isTrue();
    }
  }

  private void doOperations(int numberOfOperations) {
    var count = 0;
    while (count < numberOfOperations) {
      var index = random.nextInt(objects.size());
      var key = objects.get(index);
      assertThat(serverCQResultCache.contains(targetKey)).isTrue();

      if (random.nextBoolean()) {
        serverCQResultCache.add(key);
      } else {
        if (serverCQResultCache.contains(key)) {
          serverCQResultCache.markAsDestroyed(key);
          serverCQResultCache.remove(key, true);
        }
      }
      ++count;
    }
  }
}
