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
package org.apache.geode.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.IntStream;

import org.junit.Test;

public class UniquePortSupplierTest {

  @Test
  public void returnsUniquePorts() {
    // Create a stream that returns the same number more than once, make sure we find
    // a unique port
    var iterator = IntStream.of(0, 0, 0, 0, 0, 1).iterator();
    var supplier = new UniquePortSupplier(iterator::nextInt);
    var port0 = supplier.getAvailablePort();
    var port1 = supplier.getAvailablePort();

    assertThat(port0).isEqualTo(0);
    assertThat(port1).isEqualTo(1);
  }

  @Test
  public void getsPortsFromProvidedSupplier() {
    var expectedPort = 555;

    var supplier = new UniquePortSupplier(() -> expectedPort);
    var port = supplier.getAvailablePort();

    assertThat(port).isEqualTo(expectedPort);
  }
}
