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

package org.apache.geode.internal.tcp.pool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.tcp.InternalConnection;

public class ConnectionPoolImplTest {

  @Test
  public void claimReturnsNullWhenEmpty() {
    final ConnectionPoolImpl connectionPool = new ConnectionPoolImpl();
    final InternalDistributedMember member = mock(InternalDistributedMember.class);

    final InternalConnection connection = connectionPool.claim(member);

    assertThat(connection).isNull();
  }

  @Test
  public void claimReturnsNullForUnknownMember() {
    final ConnectionPoolImpl connectionPool = new ConnectionPoolImpl();
    final InternalDistributedMember knownMember = mock(InternalDistributedMember.class);
    final InternalConnection connectionForKnownMember = mock(InternalConnection.class);
    when(connectionForKnownMember.getRemoteAddress()).thenReturn(knownMember);
    connectionPool.add(connectionForKnownMember);
    final InternalDistributedMember unknownMember = mock(InternalDistributedMember.class);

    final InternalConnection connection = connectionPool.claim(unknownMember);

    assertThat(connection).isNull();
  }

  @Test
  public void claimReturnsConnectionForKnownMember() {
    final ConnectionPoolImpl connectionPool = new ConnectionPoolImpl();
    final InternalDistributedMember knownMember = mock(InternalDistributedMember.class);
    final InternalConnection connectionForKnownMember = mock(InternalConnection.class);
    when(connectionForKnownMember.getRemoteAddress()).thenReturn(knownMember);
    connectionPool.add(connectionForKnownMember);

    final InternalConnection connection = connectionPool.claim(knownMember);

    assertThat(connection).isNotNull();
    assertThat(connection.getRemoteAddress()).isEqualTo(knownMember);
  }

  @Test
  public void makePooledCreatesPooledConnectionAndCreatesPool() {
    final ConnectionPoolImpl connectionPool = new ConnectionPoolImpl(1);
    final InternalDistributedMember member = mock(InternalDistributedMember.class);
    final InternalConnection connection = mock(InternalConnection.class);
    when(connection.getRemoteAddress()).thenReturn(member);

    final PooledConnection pooledConnection = connectionPool.makePooled(connection);
    assertThat(pooledConnection).isNotNull();

    assertThat(connectionPool.pools).containsKey(member);
  }

  @Test
  public void relinquishClosesConnectionIfNoPoolExists() {
    final ConnectionPoolImpl connectionPool = new ConnectionPoolImpl();
    final PooledConnection pooledConnection = mock(PooledConnection.class);
    final InternalDistributedMember unknownMember = mock(InternalDistributedMember.class);
    when(pooledConnection.getRemoteAddress()).thenReturn(unknownMember);

    connectionPool.relinquish(pooledConnection);

    verify(pooledConnection).getRemoteAddress();
    verify(pooledConnection).closeOldConnection(any());
    verifyNoMoreInteractions(pooledConnection);
  }

  @Test
  public void relinquishClosesConnectionIfPoolIsFull() {
    final ConnectionPoolImpl connectionPool = new ConnectionPoolImpl(1);
    final InternalDistributedMember knownMember = mock(InternalDistributedMember.class);
    final InternalConnection connection1 = mock(InternalConnection.class);
    when(connection1.getRemoteAddress()).thenReturn(knownMember);
    final InternalConnection connection2 = mock(InternalConnection.class);
    when(connection2.getRemoteAddress()).thenReturn(knownMember);
    final PooledConnection pooledConnection1 = connectionPool.makePooled(connection1);
    final PooledConnection pooledConnection2 = connectionPool.makePooled(connection2);
    clearInvocations(connection1, connection2);

    connectionPool.relinquish(pooledConnection1);
    connectionPool.relinquish(pooledConnection2);

    verify(connection1).getRemoteAddress();
    verifyNoMoreInteractions(connection1);
    verify(connection2).getRemoteAddress();
    verify(connection2).closeOldConnection(any());
    verifyNoMoreInteractions(connection2);
  }

}
