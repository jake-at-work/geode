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
package org.apache.geode.internal.cache.tx;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.TransactionDataNotColocatedException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.ReplySender;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.DistributedCacheOperation;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EntryEventImpl.NewValueImporter;
import org.apache.geode.internal.cache.EntryEventImpl.OldValueImporter;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RemoteOperationException;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.DiskVersionTag;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * This message is used by transactions to update an entry on a transaction hosted on a remote
 * member. It is also used by non-transactional region updates that need to generate a VersionTag on
 * a remote member.
 *
 * @since GemFire 6.5
 */
public class RemotePutMessage extends RemoteOperationMessageWithDirectReply
    implements NewValueImporter, OldValueImporter {
  private static final Logger logger = LogService.getLogger();

  /** The key associated with the value that must be sent */
  private Object key;

  /** The value associated with the key that must be sent */
  private byte[] valBytes;

  private byte[] oldValBytes;

  /**
   * Used on sender side only to defer serialization until toData is called.
   */
  private transient Object valObj;

  private transient Object oldValObj;

  /** The callback arg of the operation */
  private Object cbArg;

  /** The time stamp when the value was created */
  protected long lastModified;

  /** The operation performed on the sender */
  private Operation op;

  /**
   * An additional object providing context for the operation, e.g., for BridgeServer notification
   */
  ClientProxyMembershipID bridgeContext;

  /** event identifier */
  EventID eventId;

  /**
   * for relayed messages, this is the sender of the original message. It should be used in
   * constructing events for listener notification.
   */
  InternalDistributedMember originalSender;

  /**
   * Indicates if and when the new value should be deserialized on the the receiver. Distinguishes
   * between a non-byte[] value that was serialized (DESERIALIZATION_POLICY_LAZY) and a byte[] array
   * value that didn't need to be serialized (DESERIALIZATION_POLICY_NONE). While this seems like an
   * extra data, it isn't, because serializing a byte[] causes the type (a byte) to be written in
   * the stream, AND what's better is that handling this distinction at this level reduces
   * processing for values that are byte[].
   */
  protected byte deserializationPolicy = DistributedCacheOperation.DESERIALIZATION_POLICY_NONE;

  protected boolean oldValueIsSerialized = false;

  /**
   * whether it's okay to create a new key
   */
  private boolean ifNew;

  /**
   * whether it's okay to update an existing key
   */
  private boolean ifOld;

  /**
   * Whether an old value is required in the response
   */
  private boolean requireOldValue;

  /**
   * For put to happen, the old value must be equal to this expectedOldValue.
   *
   * @see PartitionedRegion#replace(Object, Object, Object)
   */
  private Object expectedOldValue;

  private VersionTag<?> versionTag;

  private transient InternalDistributedSystem internalDs;

  /**
   * state from operateOnRegion that must be preserved for transmission from the waiting pool
   */
  transient boolean result = false;

  private boolean hasOldValue = false;

  /** whether value has delta **/
  private boolean hasDelta = false;

  /** whether new value is formed by applying delta **/
  private transient boolean applyDeltaBytes = false;

  /** delta bytes read in fromData that will be used in operate() */
  private transient byte[] deltaBytes;

  /** whether to send delta or full value **/
  private transient boolean sendDelta = false;

  private EntryEventImpl event = null;

  private boolean useOriginRemote;

  private boolean possibleDuplicate;

  protected static final short IF_NEW = UNRESERVED_FLAGS_START;
  protected static final short IF_OLD = (IF_NEW << 1);
  protected static final short REQUIRED_OLD_VAL = (IF_OLD << 1);
  protected static final short HAS_OLD_VAL = (REQUIRED_OLD_VAL << 1);
  protected static final short HAS_DELTA_BYTES = (HAS_OLD_VAL << 1);
  protected static final short USE_ORIGIN_REMOTE = (HAS_DELTA_BYTES << 1);
  protected static final short CACHE_WRITE = (USE_ORIGIN_REMOTE << 1);
  protected static final short HAS_EXPECTED_OLD_VAL = (CACHE_WRITE << 1);

  // below flags go into deserializationPolicy
  protected static final int HAS_BRIDGE_CONTEXT =
      getNextByteMask(DistributedCacheOperation.DESERIALIZATION_POLICY_END);
  protected static final int HAS_ORIGINAL_SENDER = getNextByteMask(HAS_BRIDGE_CONTEXT);
  protected static final int HAS_VERSION_TAG = getNextByteMask(HAS_ORIGINAL_SENDER);
  protected static final int HAS_CALLBACKARG = getNextByteMask(HAS_VERSION_TAG);

  /**
   * Empty constructor to satisfy {@link DataSerializer}requirements
   */
  public RemotePutMessage() {}

  protected RemotePutMessage(DistributedMember recipient, String regionPath,
      DirectReplyProcessor processor, EntryEventImpl event, final long lastModified, boolean ifNew,
      boolean ifOld, Object expectedOldValue, boolean requireOldValue, boolean useOriginRemote,
      boolean possibleDuplicate) {
    super((InternalDistributedMember) recipient, regionPath, processor);
    this.processor = processor;
    this.requireOldValue = requireOldValue;
    this.expectedOldValue = expectedOldValue;
    this.useOriginRemote = useOriginRemote;
    key = event.getKey();
    this.possibleDuplicate = possibleDuplicate;

    // useOriginRemote is true for TX single hops only as of now.
    event.setOriginRemote(useOriginRemote);

    if (event.hasNewValue()) {
      deserializationPolicy = DistributedCacheOperation.DESERIALIZATION_POLICY_LAZY;
      event.exportNewValue(this);
    } else {
      // assert that if !event.hasNewValue, then deserialization policy is NONE
      assert deserializationPolicy == DistributedCacheOperation.DESERIALIZATION_POLICY_NONE : deserializationPolicy;
    }

    // added for cqs on cache servers. rdubey


    if (event.hasOldValue()) {
      hasOldValue = true;
      event.exportOldValue(this);
    }

    this.event = event;

    cbArg = event.getRawCallbackArgument();
    this.lastModified = lastModified;
    op = event.getOperation();
    bridgeContext = event.getContext();
    eventId = event.getEventId();
    Assert.assertTrue(eventId != null);
    this.ifNew = ifNew;
    this.ifOld = ifOld;
    versionTag = event.getVersionTag();
  }

  /**
   * this is similar to send() but it selects an initialized replicate that is used to proxy the
   * message
   *
   * @param event represents the current operation
   * @param lastModified lastModified time
   * @param ifNew whether a new entry can be created
   * @param ifOld whether an old entry can be used (updates are okay)
   * @param expectedOldValue the value being overwritten is required to match this value
   * @param requireOldValue whether the old value should be returned
   * @param onlyPersistent send message to persistent members only
   * @return whether the message was successfully distributed to another member
   */
  @SuppressWarnings("unchecked")
  public static boolean distribute(EntryEventImpl event, long lastModified, boolean ifNew,
      boolean ifOld, Object expectedOldValue, boolean requireOldValue, boolean onlyPersistent) {
    boolean successful = false;
    DistributedRegion r = (DistributedRegion) event.getRegion();
    Collection<InternalDistributedMember> replicates = onlyPersistent
        ? r.getCacheDistributionAdvisor().adviseInitializedPersistentMembers().keySet()
        : r.getCacheDistributionAdvisor().adviseInitializedReplicates();
    if (replicates.isEmpty()) {
      return false;
    }
    if (replicates.size() > 1) {
      ArrayList<InternalDistributedMember> l = new ArrayList<>(replicates);
      Collections.shuffle(l);
      replicates = l;
    }
    int attempts = 0;
    if (logger.isDebugEnabled()) {
      logger.debug("performing remote put messaging for {}", event);
    }
    for (InternalDistributedMember replicate : replicates) {
      try {
        attempts++;
        final boolean posDup = (attempts > 1);
        RemotePutResponse response = send(replicate, event.getRegion(), event, lastModified, ifNew,
            ifOld, expectedOldValue, requireOldValue, false, posDup);
        PutResult result = response.waitForResult();
        event.setOldValue(result.oldValue, true/* force */);
        event.setOperation(result.op);
        if (result.versionTag != null) {
          event.setVersionTag(result.versionTag);
          if (event.getRegion().getVersionVector() != null) {
            event.getRegion().getVersionVector().recordVersion(result.versionTag.getMemberID(),
                result.versionTag);
          }
        }
        event.setInhibitDistribution(true);
        return true;

      } catch (TransactionDataNotColocatedException enfe) {
        throw enfe;

      } catch (CancelException e) {
        event.getRegion().getCancelCriterion().checkCancelInProgress(e);

      } catch (CacheException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("RemotePutMessage caught CacheException during distribution", e);
        }
        successful = true; // not a cancel-exception, so don't complain any more about it

      } catch (RegionDestroyedException | RemoteOperationException e) {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE,
              "RemotePutMessage caught an exception during distribution; retrying to another member",
              e);
        }
      }
    }
    return successful;
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  /**
   * Sends a ReplicateRegion {@link org.apache.geode.cache.Region#put(Object, Object)} message to
   * the recipient
   *
   * @param recipient the member to which the put message is sent
   * @param r the PartitionedRegion for which the put was performed
   * @param event the event prompting this message
   * @param ifNew whether a new entry must be created
   * @param ifOld whether an old entry must be updated (no creates)
   * @return the processor used to await acknowledgement that the update was sent, or null to
   *         indicate that no acknowledgement will be sent
   * @throws RemoteOperationException if the peer is no longer available
   */
  public static RemotePutResponse txSend(DistributedMember recipient, InternalRegion r,
      EntryEventImpl event, final long lastModified, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue) throws RemoteOperationException {
    return send(recipient, r, event, lastModified, ifNew, ifOld, expectedOldValue, requireOldValue,
        true, false);
  }

  /**
   * Sends a ReplicateRegion {@link org.apache.geode.cache.Region#put(Object, Object)} message to
   * the recipient
   *
   * @param recipient the member to which the put message is sent
   * @param r the region for which the put was performed
   * @param event the event prompting this message
   * @param ifNew whether a new entry must be created
   * @param ifOld whether an old entry must be updated (no creates)
   * @param useOriginRemote whether the receiver should consider the event local or remote
   * @return the processor used to await acknowledgement that the update was sent, or null to
   *         indicate that no acknowledgement will be sent
   * @throws RemoteOperationException if the peer is no longer available
   */
  public static RemotePutResponse send(DistributedMember recipient, InternalRegion r,
      EntryEventImpl event, final long lastModified, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, boolean useOriginRemote,
      boolean possibleDuplicate) throws RemoteOperationException {

    RemotePutResponse processor = new RemotePutResponse(r.getSystem(), recipient, false);

    RemotePutMessage m =
        new RemotePutMessage(recipient, r.getFullPath(), processor, event, lastModified, ifNew,
            ifOld, expectedOldValue, requireOldValue, useOriginRemote, possibleDuplicate);
    m.setInternalDs(r.getSystem());
    m.setSendDelta(true);

    processor.setRemotePutMessage(m);

    Set<?> failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new RemoteOperationException(
          String.format("Failed sending < %s >", m));
    }
    return processor;
  }

  public Object getKey() {
    return key;
  }

  public void setKey(Object key) {
    this.key = key;
  }

  public byte[] getValBytes() {
    return valBytes;
  }

  public byte[] getOldValueBytes() {

    return oldValBytes;
  }

  private void setValBytes(byte[] valBytes) {
    this.valBytes = valBytes;
  }

  private void setOldValBytes(byte[] valBytes) {
    oldValBytes = valBytes;
  }

  private Object getOldValObj() {
    return oldValObj;
  }

  private void setValObj(Object o) {
    valObj = o;
  }

  private void setOldValObj(Object o) {
    oldValObj = o;
  }

  public Object getCallbackArg() {
    return cbArg;
  }

  protected Operation getOperation() {
    return op;
  }

  @Override
  public void setOperation(Operation operation) {
    op = operation;
  }

  /**
   * sets the instance variable hasOldValue to the giving boolean value.
   */
  @Override
  public void setHasOldValue(final boolean value) {
    hasOldValue = value;
  }

  @Override
  public int getDSFID() {
    return R_PUT_MESSAGE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    setKey(DataSerializer.readObject(in));

    final int extraFlags = in.readUnsignedByte();
    deserializationPolicy =
        (byte) (extraFlags & DistributedCacheOperation.DESERIALIZATION_POLICY_MASK);
    cbArg = DataSerializer.readObject(in);
    lastModified = in.readLong();
    op = Operation.fromOrdinal(in.readByte());
    if ((extraFlags & HAS_BRIDGE_CONTEXT) != 0) {
      bridgeContext = DataSerializer.readObject(in);
    }
    if ((extraFlags & HAS_ORIGINAL_SENDER) != 0) {
      originalSender = DataSerializer.readObject(in);
    }
    eventId = new EventID();
    InternalDataSerializer.invokeFromData(eventId, in);

    if ((flags & HAS_EXPECTED_OLD_VAL) != 0) {
      expectedOldValue = DataSerializer.readObject(in);
    }

    if (hasOldValue) {
      oldValueIsSerialized = (in.readByte() == 1);
      setOldValBytes(DataSerializer.readByteArray(in));
    }
    setValBytes(DataSerializer.readByteArray(in));
    if ((flags & HAS_DELTA_BYTES) != 0) {
      applyDeltaBytes = true;
      deltaBytes = DataSerializer.readByteArray(in);
    }
    if ((extraFlags & HAS_VERSION_TAG) != 0) {
      versionTag = DataSerializer.readObject(in);
    }
  }

  @Override
  protected void setFlags(short flags, DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.setFlags(flags, in, context);
    ifNew = (flags & IF_NEW) != 0;
    ifOld = (flags & IF_OLD) != 0;
    requireOldValue = (flags & REQUIRED_OLD_VAL) != 0;
    hasOldValue = (flags & HAS_OLD_VAL) != 0;
    useOriginRemote = (flags & USE_ORIGIN_REMOTE) != 0;
  }

  @Override
  public EventID getEventID() {
    return eventId;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    hasDelta = false;
    super.toData(out, context);
    DataSerializer.writeObject(getKey(), out);

    int extraFlags = deserializationPolicy;
    if (bridgeContext != null) {
      extraFlags |= HAS_BRIDGE_CONTEXT;
    }
    if (originalSender != null) {
      extraFlags |= HAS_ORIGINAL_SENDER;
    }
    if (versionTag != null) {
      extraFlags |= HAS_VERSION_TAG;
    }
    out.writeByte(extraFlags);

    DataSerializer.writeObject(getCallbackArg(), out);
    out.writeLong(lastModified);
    out.writeByte(op.ordinal);
    if (bridgeContext != null) {
      DataSerializer.writeObject(bridgeContext, out);
    }
    if (originalSender != null) {
      DataSerializer.writeObject(originalSender, out);
    }
    InternalDataSerializer.invokeToData(eventId, out);

    if (expectedOldValue != null) {
      DataSerializer.writeObject(expectedOldValue, out);
    }
    // this will be on wire for cqs old value generations.
    if (hasOldValue) {
      out.writeByte(oldValueIsSerialized ? 1 : 0);
      byte policy = DistributedCacheOperation.valueIsToDeserializationPolicy(oldValueIsSerialized);
      DistributedCacheOperation.writeValue(policy, getOldValObj(), getOldValueBytes(), out);
    }
    DistributedCacheOperation.writeValue(deserializationPolicy, valObj, getValBytes(),
        out);
    if (event.getDeltaBytes() != null) {
      DataSerializer.writeByteArray(event.getDeltaBytes(), out);
    }
    if (versionTag != null) {
      DataSerializer.writeObject(versionTag, out);
    }
  }

  @Override
  protected short computeCompressedShort() {
    short s = super.computeCompressedShort();
    if (ifNew) {
      s |= IF_NEW;
    }
    if (ifOld) {
      s |= IF_OLD;
    }
    if (requireOldValue) {
      s |= REQUIRED_OLD_VAL;
    }
    if (hasOldValue) {
      s |= HAS_OLD_VAL;
    }
    if (event.getDeltaBytes() != null) {
      s |= HAS_DELTA_BYTES;
    }
    if (expectedOldValue != null) {
      s |= HAS_EXPECTED_OLD_VAL;
    }
    if (useOriginRemote) {
      s |= USE_ORIGIN_REMOTE;
    }
    if (possibleDuplicate) {
      s |= POS_DUP;
    }
    return s;
  }

  /**
   * This method is called upon receipt and make the desired changes to the Replicate Region. Note:
   * It is very important that this message does NOT cause any deadlocks as the sender will wait
   * indefinitely for the acknowledgement
   */
  @Override
  protected boolean operateOnRegion(ClusterDistributionManager dm, LocalRegion r, long startTime)
      throws EntryExistsException, RemoteOperationException {
    setInternalDs(r.getSystem());// set the internal DS. Required to
                                 // checked DS level delta-enabled property
                                 // while sending delta
    boolean sendReply = true;

    InternalDistributedMember eventSender = originalSender;
    if (eventSender == null) {
      eventSender = getSender();
    }
    EntryEventImpl eei = EntryEventImpl.create(r, getOperation(), getKey(), null, /* newValue */
        getCallbackArg(),
        useOriginRemote, /* originRemote - false to force distribution in buckets */
        eventSender, true/* generateCallbacks */, false/* initializeId */);
    event = eei;
    if (versionTag != null) {
      versionTag.replaceNullIDs(getSender());
      event.setVersionTag(versionTag);
    }
    event.setCausedByMessage(this);

    event.setPossibleDuplicate(possibleDuplicate);
    if (bridgeContext != null) {
      event.setContext(bridgeContext);
    }

    Assert.assertTrue(eventId != null);
    event.setEventId(eventId);

    // added for cq procesing
    if (hasOldValue) {
      if (oldValueIsSerialized) {
        event.setSerializedOldValue(getOldValueBytes());
      } else {
        event.setOldValue(getOldValueBytes());
      }
    }

    if (applyDeltaBytes) {
      event.setNewValue(valObj);
      event.setDeltaBytes(deltaBytes);
    } else {
      switch (deserializationPolicy) {
        case DistributedCacheOperation.DESERIALIZATION_POLICY_LAZY:
          event.setSerializedNewValue(getValBytes());
          break;
        case DistributedCacheOperation.DESERIALIZATION_POLICY_NONE:
          event.setNewValue(getValBytes());
          break;
        default:
          throw new AssertionError("unknown deserialization policy: " + deserializationPolicy);
      }
    }

    try {
      result = r.getDataView().putEntry(event, ifNew, ifOld, expectedOldValue,
          requireOldValue, lastModified, true);

      if (!result) { // make sure the region hasn't gone away
        r.checkReadiness();
        if (!ifNew && !ifOld) {
          // no reason to be throwing an exception, so let's retry
          RemoteOperationException ex = new RemoteOperationException(
              "unable to perform put, but operation should not fail");
          sendReply(getSender(), getProcessorId(), dm, new ReplyException(ex), r, startTime);
          return false;
        }
      }
    } catch (CacheWriterException cwe) {
      sendReply(getSender(), getProcessorId(), dm, new ReplyException(cwe), r, startTime);
      return false;
    }

    setOperation(event.getOperation()); // set operation for reply message

    if (sendReply) {
      sendReply(getSender(), getProcessorId(), dm, null, r, startTime, event);
    }
    return false;
  }


  protected void sendReply(InternalDistributedMember member, int procId, DistributionManager dm,
      ReplyException ex, LocalRegion pr, long startTime, EntryEventImpl event) {
    PutReplyMessage.send(member, procId, getReplySender(dm), result, getOperation(), ex, this,
        event);
  }

  @Override
  protected void sendReply(InternalDistributedMember member, int procId, DistributionManager dm,
      ReplyException ex, InternalRegion r, long startTime) {
    PutReplyMessage.send(member, procId, getReplySender(dm), result, getOperation(), ex, this,
        null);
  }

  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; key=").append(getKey()).append("; value=");
    // buff.append(getValBytes());
    buff.append(getValBytes() == null ? valObj : "(" + getValBytes().length + " bytes)");
    buff.append("; callback=").append(cbArg).append("; op=").append(op);
    if (originalSender != null) {
      buff.append("; originalSender=").append(originalSender);
    }
    if (bridgeContext != null) {
      buff.append("; bridgeContext=").append(bridgeContext);
    }
    if (eventId != null) {
      buff.append("; eventId=").append(eventId);
    }
    buff.append("; ifOld=").append(ifOld).append("; ifNew=").append(ifNew).append("; op=")
        .append(getOperation());
    buff.append("; hadOldValue=").append(hasOldValue);
    if (hasOldValue) {
      byte[] ov = getOldValueBytes();
      if (ov != null) {
        buff.append("; oldValueLength=").append(ov.length);
      }
    }
    buff.append("; deserializationPolicy=");
    buff.append(
        DistributedCacheOperation.deserializationPolicyToString(deserializationPolicy));
    buff.append("; hasDelta=");
    buff.append(hasDelta);
    buff.append("; sendDelta=");
    buff.append(sendDelta);
    buff.append("; isDeltaApplied=");
    buff.append(applyDeltaBytes);
  }

  public InternalDistributedSystem getInternalDs() {
    return internalDs;
  }

  public void setInternalDs(InternalDistributedSystem internalDs) {
    this.internalDs = internalDs;
  }

  public static class PutReplyMessage extends ReplyMessage implements OldValueImporter {

    static final byte FLAG_RESULT = 0x01;
    static final byte FLAG_HASVERSION = 0x02;
    static final byte FLAG_PERSISTENT = 0x04;

    /** Result of the Put operation */
    boolean result;

    /** The Operation actually performed */
    Operation op;

    /**
     * Set to true by the import methods if the oldValue is already serialized. In that case toData
     * should just copy the bytes to the stream. In either case fromData just calls readObject.
     */
    private transient boolean oldValueIsSerialized;

    /**
     * Old value in serialized form: either a byte[] or CachedDeserializable, or null if not set.
     */
    private Object oldValue;

    /**
     * version tag for concurrency control
     */
    VersionTag<?> versionTag;

    @Override
    public boolean getInlineProcess() {
      return true;
    }

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public PutReplyMessage() {}

    // unit tests may call this constructor
    PutReplyMessage(int processorId, boolean result, Operation op, ReplyException ex,
        Object oldValue, VersionTag<?> versionTag) {
      super();
      this.op = op;
      this.result = result;
      setProcessorId(processorId);
      setException(ex);
      this.oldValue = oldValue;
      this.versionTag = versionTag;
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient, int processorId, ReplySender dm,
        boolean result, Operation op, ReplyException ex, RemotePutMessage sourceMessage,
        EntryEventImpl event) {
      Assert.assertTrue(recipient != null, "PutReplyMessage NULL recipient");
      PutReplyMessage m = new PutReplyMessage(processorId, result, op, ex, null,
          event != null ? event.getVersionTag() : null);

      if (sourceMessage.requireOldValue && event != null) {
        event.exportOldValue(m);
      }

      m.setRecipient(recipient);
      dm.putOutgoing(m);
    }

    /**
     * Processes this message. This method is invoked by the receiver of the message.
     *
     * @param dm the distribution manager that is processing the message.
     */
    @Override
    public void process(final DistributionManager dm, final ReplyProcessor21 rp) {
      final long startTime = getTimestamp();
      if (logger.isDebugEnabled()) {
        logger.debug("Processing {}", this);
      }
      if (rp == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("PutReplyMessage processor not found");
        }
        return;
      }
      if (versionTag != null) {
        versionTag.replaceNullIDs(getSender());
      }
      if (rp instanceof RemotePutResponse) {
        RemotePutResponse processor = (RemotePutResponse) rp;
        processor.setResponse(this);
      }
      rp.process(this);

      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE, "{} processed {}", rp, this);
      }
      dm.getStats().incReplyMessageTime(NanoTimer.getTime() - startTime);
    }

    /**
     * Return oldValue as a byte[] or as a CachedDeserializable. This method used to deserialize a
     * CachedDeserializable but that is too soon. This method is called during message processing.
     * The deserialization needs to be deferred until we get back to the application thread which
     * happens for this oldValue when they call EntryEventImpl.getOldValue.
     */
    public Object getOldValue() {
      // oldValue field is in serialized form, either a CachedDeserializable,
      // a byte[], or null if not set
      return oldValue;
    }

    @Override
    public int getDSFID() {
      return R_PUT_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      byte flags = (byte) (in.readByte() & 0xff);
      result = (flags & FLAG_RESULT) != 0;
      op = Operation.fromOrdinal(in.readByte());
      oldValue = DataSerializer.readObject(in);
      if ((flags & FLAG_HASVERSION) != 0) {
        boolean persistentTag = (flags & FLAG_PERSISTENT) != 0;
        versionTag = VersionTag.create(persistentTag, in);
      }
    }

    public static void oldValueToData(DataOutput out, Object ov, boolean ovIsSerialized)
        throws IOException {
      if (ovIsSerialized && ov != null) {
        byte[] oldValueBytes;
        if (ov instanceof byte[]) {
          oldValueBytes = (byte[]) ov;
          DataSerializer.writeObject(new VMCachedDeserializable(oldValueBytes), out);
        } else if (ov instanceof CachedDeserializable) {
          DataSerializer.writeObject(ov, out);
        } else {
          oldValueBytes = EntryEventImpl.serialize(ov);
          DataSerializer.writeObject(new VMCachedDeserializable(oldValueBytes), out);
        }
      } else {
        DataSerializer.writeObject(ov, out);
      }

    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      byte flags = 0;
      if (result) {
        flags |= FLAG_RESULT;
      }
      if (versionTag != null) {
        flags |= FLAG_HASVERSION;
      }
      if (versionTag instanceof DiskVersionTag) {
        flags |= FLAG_PERSISTENT;
      }
      out.writeByte(flags);
      out.writeByte(op.ordinal);
      oldValueToData(out, getOldValue(), oldValueIsSerialized);
      if (versionTag != null) {
        InternalDataSerializer.invokeToData(versionTag, out);
      }
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("PutReplyMessage ").append("processorid=").append(processorId)
          .append(" returning ").append(result).append(" op=").append(op).append(" exception=")
          .append(getException());
      if (versionTag != null) {
        sb.append(" version=").append(versionTag);
      }
      return sb.toString();
    }

    @Override
    public boolean prefersOldSerialized() {
      return true;
    }

    @Override
    public boolean isCachedDeserializableValueOk() {
      return true;
    }

    @Override
    public void importOldObject(Object ov,
        boolean isSerialized) {
      oldValueIsSerialized = isSerialized;
      oldValue = ov;
    }

    @Override
    public void importOldBytes(byte[] ov, boolean isSerialized) {
      importOldObject(ov, isSerialized);
    }
  }

  /**
   * A processor to capture the value returned by {@link RemotePutMessage}
   *
   * @since GemFire 5.1
   */
  public static class RemotePutResponse extends RemoteOperationResponse {
    private volatile boolean returnValue;
    private volatile Operation op;
    private volatile Object oldValue;
    private RemotePutMessage putMessage;
    private VersionTag<?> versionTag;

    public RemotePutResponse(InternalDistributedSystem ds, DistributedMember recipient,
        boolean register) {
      super(ds, (InternalDistributedMember) recipient, register);
    }


    public void setRemotePutMessage(RemotePutMessage putMessage) {
      this.putMessage = putMessage;
    }

    public RemotePutMessage getRemotePutMessage() {
      return putMessage;
    }

    public void setResponse(PutReplyMessage msg) {
      returnValue = msg.result;
      op = msg.op;
      oldValue = msg.getOldValue();
      versionTag = msg.versionTag;
    }

    /**
     * @return the result of the remote put operation
     * @throws RemoteOperationException if the peer is no longer available
     * @throws CacheException if the peer generates an error
     */
    public PutResult waitForResult() throws CacheException, RemoteOperationException {
      waitForRemoteResponse();
      if (op == null) {
        throw new RemoteOperationException(
            "did not receive a valid reply");
      }
      return new PutResult(returnValue, op, oldValue, versionTag);
    }

  }

  public static class PutResult {
    /** the result of the put operation */
    public boolean returnValue;
    /** the actual operation performed (CREATE/UPDATE) */
    public Operation op;

    /** the old value, or null if not set */
    public Object oldValue;

    /** the concurrency control version tag */
    public VersionTag<?> versionTag;

    public PutResult(boolean flag, Operation actualOperation, Object oldValue,
        VersionTag<?> versionTag) {
      returnValue = flag;
      op = actualOperation;
      this.oldValue = oldValue;
      this.versionTag = versionTag;
    }
  }

  public void setSendDelta(boolean sendDelta) {
    this.sendDelta = sendDelta;
  }

  @Override
  public boolean prefersNewSerialized() {
    return true;
  }

  private void setDeserializationPolicy(boolean isSerialized) {
    if (!isSerialized) {
      deserializationPolicy = DistributedCacheOperation.DESERIALIZATION_POLICY_NONE;
    }
  }

  @Override
  public void importNewObject(Object nv, boolean isSerialized) {
    setDeserializationPolicy(isSerialized);
    setValObj(nv);
  }

  @Override
  public void importNewBytes(byte[] nv, boolean isSerialized) {
    setDeserializationPolicy(isSerialized);
    setValBytes(nv);
  }

  @Override
  public boolean prefersOldSerialized() {
    return true;
  }

  @Override
  public boolean isCachedDeserializableValueOk() {
    return false;
  }

  private void setOldValueIsSerialized(boolean isSerialized) {
    // Defer serialization until toData is called.
    // VALUE_IS_BYTES;
    oldValueIsSerialized = isSerialized; // VALUE_IS_SERIALIZED_OBJECT;
  }

  @Override
  public void importOldObject(Object ov, boolean isSerialized) {
    setOldValueIsSerialized(isSerialized);
    // Defer serialization until toData is called.
    setOldValObj(ov);
  }

  @Override
  public void importOldBytes(byte[] ov, boolean isSerialized) {
    setOldValueIsSerialized(isSerialized);
    setOldValBytes(ov);
  }
}
