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

package org.apache.geode.cache.wan.internal.txbatch;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.cache.TransactionId;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

public class TxBatchMetadata implements DataSerializableFixedID {

  private @NotNull TransactionId transactionId;
  private boolean lastEvent;

  public TxBatchMetadata(final @NotNull TransactionId transactionId, final boolean lastEvent) {
    // TODO jbarrett is there a way to reduce this by assuming the event memberId and tx memberId
    // are always the same?
    this.transactionId = transactionId;
    this.lastEvent = lastEvent;
  }

  public @NotNull TransactionId getTransactionId() {
    return transactionId;
  }

  public boolean isLastEvent() {
    return lastEvent;
  }

  @Override
  public int getDSFID() {
    // TODO jbarrett new id
    return 0;
  }

  @Override
  public void toData(final DataOutput out, final SerializationContext context) throws IOException {
    context.getSerializer().writeObject(transactionId, out);
    out.writeBoolean(lastEvent);
  }

  @Override
  public void fromData(final DataInput in, final DeserializationContext context)
      throws IOException, ClassNotFoundException {
    transactionId = context.getDeserializer().readObject(in);
    lastEvent = in.readBoolean();
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return new KnownVersion[0];
  }

  @Override
  public String toString() {
    return "TxBatchMetaData{" +
        "transactionId=" + transactionId +
        ", lastEvent=" + lastEvent +
        '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TxBatchMetadata that = (TxBatchMetadata) o;
    return lastEvent == that.lastEvent && transactionId.equals(that.transactionId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transactionId, lastEvent);
  }
}
