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

package org.apache.geode.internal.cache.tier.sockets;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.Maps;
import org.junit.Test;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.InternalDataSerializer;

public class CqNameToOpHashMapIntegrationTest {
  /**
   * This test ensures that we can safely mutate the CqNameToOpHashMap while it is being
   * serialized in another thread. The use case for this is that we repopulate this map
   * during client registration, which can happen concurrently with a GII which causes
   * serialization to occur if this map is referenced by any of the client subscription
   * queues. This integration test does not exercise this full system level interaction,
   * but rather does the minimum necessary to prove that map mutation and serialization
   * can occur concurrently without any issues such as size mismatches or
   * ConcurrentModificationExceptions.
   */
  @Test
  public void testSendToWhileConcurrentlyModifyingMapContentsAndVerifyProperSerialization()
      throws IOException, ClassNotFoundException, InterruptedException, ExecutionException,
      TimeoutException {
    final var numEntries = 1000000;

    var originalCqNameToOpHashMap =
        new ClientUpdateMessageImpl.CqNameToOpHashMap(numEntries);
    var modifiedCqNameToOpHashMap =
        new ClientUpdateMessageImpl.CqNameToOpHashMap(numEntries);

    for (var i = 0; i < numEntries; ++i) {
      originalCqNameToOpHashMap.add(String.valueOf(i), i);
      modifiedCqNameToOpHashMap.add(String.valueOf(i), i);
    }

    var removeFromHashMapTask = CompletableFuture.runAsync(() -> {
      for (var i = 0; i < numEntries; ++i) {
        modifiedCqNameToOpHashMap.remove(String.valueOf(i), i);
      }
    });

    var serializeReconstructHashMapTask = CompletableFuture.runAsync(() -> {
      try {
        var byteArrayOutputStream = new ByteArrayOutputStream();
        var outputStream = new DataOutputStream(byteArrayOutputStream);

        modifiedCqNameToOpHashMap.sendTo(outputStream);

        var byteArrayInputStream =
            new ByteArrayInputStream(byteArrayOutputStream.toByteArray());

        var inputStream = new DataInputStream(byteArrayInputStream);

        var typeByte = inputStream.readByte();
        var cqNamesSize = InternalDataSerializer.readArrayLength(inputStream);

        var reconstructedCqNameToOpHashMap =
            new ClientUpdateMessageImpl.CqNameToOpHashMap(cqNamesSize);

        for (var j = 0; j < cqNamesSize; j++) {
          String cqNamesKey = DataSerializer.readObject(inputStream);
          var cqNamesValue = DataSerializer.<Integer>readObject(inputStream);
          reconstructedCqNameToOpHashMap.add(cqNamesKey, cqNamesValue);
        }

        // The reconstructed map should have some subset of the entries in the cqNameToOpHashMap,
        // but the specific contents will depend on timing with the removeFromHashMap task.
        var reconstructedVersusOriginalHashMapDifference =
            Maps.difference(reconstructedCqNameToOpHashMap, originalCqNameToOpHashMap);
        assertThat(reconstructedVersusOriginalHashMapDifference.entriesInCommon().size() >= 0)
            .isTrue();
        assertThat(reconstructedVersusOriginalHashMapDifference.entriesDiffering().size() == 0)
            .isTrue();
      } catch (Exception ex) {
        throw new RuntimeException("Failed to serialize/deserialize the CqNameToOpHashMap", ex);
      }
    });

    CompletableFuture.allOf(removeFromHashMapTask, serializeReconstructHashMapTask).get(1,
        TimeUnit.MINUTES);
  }
}
