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

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.List;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.tcp.Connection;
import org.apache.geode.internal.tcp.ConnectionException;
import org.apache.geode.internal.tcp.InternalConnection;

public class PooledConnectionImpl implements PooledConnection {
  private final ConnectionPool connectionPool;
  private final InternalConnection connection;

  public PooledConnectionImpl(@NotNull final ConnectionPool connectionPool,
      @NotNull final InternalConnection connection) {
    this.connectionPool = connectionPool;
    this.connection = connection;
  }

  @Override
  public boolean isSharedResource() {
    return connection.isSharedResource();
  }

  @Override
  public void readAck(
      final @NotNull DirectReplyProcessor processor)
      throws SocketTimeoutException, ConnectionException {
    connection.readAck(processor);
  }

  @Override
  public @Nullable InternalDistributedMember getRemoteAddress() {
    return connection.getRemoteAddress();
  }

  @Override
  public void setInUse(final boolean inUse, final long startTime, final long ackWaitThreshold,
      final long ackSAThreshold,
      final @Nullable List<@NotNull Connection> connectionGroup) {
    connection.setInUse(inUse, startTime, ackWaitThreshold, ackSAThreshold, connectionGroup);

    // TODO assert connection in correct states for pooling.

    if (!inUse) {
      connectionPool.relinquish(this);
    }
  }

  @Override
  public boolean isStopped() {
    return connection.isStopped();
  }

  @Override
  public boolean isTimedOut() {
    return connection.isTimedOut();
  }

  @Override
  public boolean isConnected() {
    return connection.isConnected();
  }

  @Override
  public int getSendBufferSize() {
    return connection.getSendBufferSize();
  }

  @Override
  public void setIdleTimeoutTask(final SystemTimer.@NotNull SystemTimerTask task) {
    connection.setIdleTimeoutTask(task);
  }

  @Override
  public boolean checkForIdleTimeout() {
    return connection.checkForIdleTimeout();
  }

  @Override
  public void cleanUpOnIdleTaskCancel() {
    connection.cleanUpOnIdleTaskCancel();
  }

  @Override
  public void requestClose(final @NotNull String reason) {
    connection.requestClose(reason);
  }

  @Override
  public boolean isClosing() {
    return connection.isClosing();
  }

  @Override
  public void closePartialConnect(final @NotNull String reason, final boolean beingSick) {
    connection.closePartialConnect(reason, beingSick);
  }

  @Override
  public void closeForReconnect(final @NotNull String reason) {
    connection.closeForReconnect(reason);
  }

  @Override
  public void closeOldConnection(final @NotNull String reason) {
    connection.closeOldConnection(reason);
  }

  @Override
  public void sendPreserialized(final @NotNull ByteBuffer buffer, final boolean cacheContentChanges,
      final @Nullable DistributionMessage msg)
      throws IOException, ConnectionException {
    connection.sendPreserialized(buffer, cacheContentChanges, msg);
  }

  @Override
  public void scheduleAckTimeouts() {
    connection.scheduleAckTimeouts();
  }

  @Override
  public boolean isSocketClosed() {
    return connection.isSocketClosed();
  }

  @Override
  public boolean isReceiverStopped() {
    return connection.isReceiverStopped();
  }

  @Override
  public @Nullable KnownVersion getRemoteVersion() {
    return connection.getRemoteVersion();
  }

  @Override
  public boolean getOriginatedHere() {
    return connection.getOriginatedHere();
  }

  @Override
  public boolean getPreserveOrder() {
    return connection.getPreserveOrder();
  }

  @Override
  public long getUniqueId() {
    return connection.getUniqueId();
  }

  @Override
  public long getMessagesReceived() {
    return connection.getMessagesReceived();
  }

  @Override
  public long getMessagesSent() {
    return connection.getMessagesSent();
  }

  @Override
  public void extendAckSevereAlertTimeout() {
    connection.extendAckSevereAlertTimeout();
  }

  @Override
  public String toString() {
    return "PooledConnection{" + connection + '}';
  }
}
