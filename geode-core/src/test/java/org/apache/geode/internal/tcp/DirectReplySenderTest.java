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

package org.apache.geode.internal.tcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.mock;

import org.junit.Test;

public class DirectReplySenderTest {

  @Test
  public void getConnectionsReturnsMutableListOfOne() {
    final var connection = mock(Connection.class);
    final var directReplySender = new DirectReplySender(connection);
    final var connections = directReplySender.getConnections();
    assertThat(connections).containsExactly(connection);
    assertThatNoException().isThrownBy(() -> {
      final var iterator = connections.iterator();
      iterator.next();
      iterator.remove();
    });
    assertThat(connections).isEmpty();
  }

}
