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
package org.apache.geode.internal.cache.persistence;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.geode.DataSerializable;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.InternalDataSerializer;

public class PersistentMembershipView implements DataSerializable {
  private Set<PersistentMemberID> offlineMembers;
  private Map<InternalDistributedMember, PersistentMemberID> onlineMembers;
  private Set<PersistentMemberPattern> revokedMembers;

  public PersistentMembershipView() {

  }

  public PersistentMembershipView(Set<PersistentMemberID> offlineMembers,
      Map<InternalDistributedMember, PersistentMemberID> onlineMembers,
      Set<PersistentMemberPattern> revokedMembers) {
    this.offlineMembers = offlineMembers;
    this.onlineMembers = onlineMembers;
    this.revokedMembers = revokedMembers;
  }

  public Set<PersistentMemberID> getOfflineMembers() {
    return offlineMembers;
  }

  public Map<InternalDistributedMember, PersistentMemberID> getOnlineMembers() {
    return onlineMembers;
  }

  public Set<PersistentMemberPattern> getRevokedMembers() {
    return revokedMembers;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.DataSerializable#fromData(java.io.DataInput)
   */
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    var offlineSize = in.readInt();
    offlineMembers = new HashSet<>(offlineSize);
    for (var i = 0; i < offlineSize; i++) {
      var id = new PersistentMemberID();
      InternalDataSerializer.invokeFromData(id, in);
      offlineMembers.add(id);
    }

    var onlineSize = in.readInt();
    onlineMembers = new HashMap<>(onlineSize);
    for (var i = 0; i < onlineSize; i++) {
      var member = new InternalDistributedMember();
      InternalDataSerializer.invokeFromData(member, in);
      var id = new PersistentMemberID();
      InternalDataSerializer.invokeFromData(id, in);
      onlineMembers.put(member, id);
    }

    var revokedSized = in.readInt();
    revokedMembers = new HashSet<>(revokedSized);
    for (var i = 0; i < revokedSized; i++) {
      var pattern = new PersistentMemberPattern();
      InternalDataSerializer.invokeFromData(pattern, in);
      revokedMembers.add(pattern);
    }


  }

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeInt(offlineMembers.size());
    for (var member : offlineMembers) {
      InternalDataSerializer.invokeToData(member, out);
    }
    out.writeInt(onlineMembers.size());
    for (var entry : onlineMembers
        .entrySet()) {
      InternalDataSerializer.invokeToData(entry.getKey(), out);
      InternalDataSerializer.invokeToData(entry.getValue(), out);
    }

    out.writeInt(revokedMembers.size());
    for (var revoked : revokedMembers) {
      InternalDataSerializer.invokeToData(revoked, out);
    }
  }

  @Override
  public String toString() {
    return "PersistentMembershipView[offline=" + offlineMembers + ",online=" + onlineMembers
        + ", revoked=" + revokedMembers + "]";
  }
}
