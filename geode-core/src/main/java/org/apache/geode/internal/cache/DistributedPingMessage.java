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
package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

public class DistributedPingMessage extends HighPriorityDistributionMessage {

  private ClientProxyMembershipID proxyID;

  public DistributedPingMessage() {
    // no-arg constructor for serialization
  }

  public DistributedPingMessage(DistributedMember targetServer, ClientProxyMembershipID proxyID) {
    super();
    this.proxyID = proxyID;
    setRecipient((InternalDistributedMember) targetServer);
  }

  @Override
  protected void process(ClusterDistributionManager dm) {
    var chm = ClientHealthMonitor.getInstance();
    if (chm != null) {
      chm.receivedPing(proxyID);
    }
  }

  @Override
  public void toData(DataOutput out, SerializationContext context) throws IOException {
    super.toData(out, context);
    context.getSerializer().writeObject(proxyID, out);
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    proxyID = context.getDeserializer().readObject(in);
  }

  @Override
  public int getDSFID() {
    return DISTRIBUTED_PING_MESSAGE;
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  @Override
  public String toString() {
    return super.toString() + "; proxyId=" + proxyID;
  }

  @VisibleForTesting
  protected ClientProxyMembershipID getProxyID() {
    return proxyID;
  }
}
