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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.TransactionDataNodeHasDepartedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ReliableReplyException;
import org.apache.geode.distributed.internal.ReliableReplyProcessor21;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.internal.cache.partitioned.BucketId;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.cache.tx.TXRegionStub;

/**
 * TXStateStub lives on the accessor node when we are remoting a transaction. It is a stub for
 * {@link TXState}.
 */
public abstract class TXStateStub implements TXStateInterface {

  protected final DistributedMember target;
  protected final TXStateProxy proxy;
  protected Runnable internalAfterSendRollback = null;
  protected Runnable internalAfterSendCommit = null;

  Map<Region<?, ?>, TXRegionStub> regionStubs = new HashMap<>();

  protected TXStateStub(TXStateProxy proxy, DistributedMember target) {
    this.target = target;
    this.proxy = proxy;
  }

  @Override
  public void precommit()
      throws CommitConflictException, UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        String.format("precommit() operation %s meant for Dist Tx is not supported",
            "precommit"));
  }

  /**
   * Implemented in subclasses for Peer vs. Client
   */
  @Override
  public abstract void commit() throws CommitConflictException;

  protected abstract void validateRegionCanJoinTransaction(InternalRegion region)
      throws TransactionException;

  protected abstract TXRegionStub generateRegionStub(InternalRegion region);

  @Override
  public abstract void rollback();

  @Override
  public abstract void afterCompletion(int status);

  @Override
  public void beforeCompletion() {
    // note that this class must do distribution as it is used as the stub class in some situations
    ReliableReplyProcessor21 response = JtaBeforeCompletionMessage.send(proxy.getCache(),
        proxy.getTxId().getUniqId(), getOriginatingMember(), target);
    try {
      response.waitForReliableDelivery();
    } catch (ReliableReplyException e) {
      throw new TransactionDataNodeHasDepartedException(e);
    } catch (ReplyException e) {
      e.handleCause();
    } catch (InterruptedException ignored) {
    }
  }

  /**
   * Get or create a TXRegionStub for the given region. For regions that are new to the tx, we
   * validate their eligibility.
   *
   * @param region The region to involve in the tx.
   * @return existing or new stub for region
   */
  protected TXRegionStub getTXRegionStub(InternalRegion region) {
    TXRegionStub stub = regionStubs.get(region);
    if (stub == null) {
      /*
       * validate whether this region is legit or not
       */
      validateRegionCanJoinTransaction(region);
      stub = generateRegionStub(region);
      regionStubs.put(region, stub);
    }
    return stub;
  }

  public Map<Region<?, ?>, TXRegionStub> getRegionStubs() {
    return regionStubs;
  }

  @Override
  public String toString() {
    return getClass() + "@" + System.identityHashCode(this)
        + " target node: " + target;
  }


  @Override
  public void destroyExistingEntry(EntryEventImpl event, boolean cacheWrite,
      Object expectedOldValue) throws EntryNotFoundException {
    if (event.getOperation().isLocal()) {
      throw new UnsupportedOperationInTransactionException(
          "localDestroy() is not allowed in a transaction");
    }
    TXRegionStub rs = getTXRegionStub(event.getRegion());
    rs.destroyExistingEntry(event, cacheWrite, expectedOldValue);
  }

  @Override
  public long getBeginTime() {
    return 0;
  }

  @Override
  public InternalCache getCache() {
    return proxy.getCache();
  }

  @Override
  public int getChanges() {
    return 0;
  }

  @Override
  public Object getDeserializedValue(KeyInfo keyInfo, LocalRegion localRegion, boolean updateStats,
      boolean disableCopyOnRead, boolean preferCD, EntryEventImpl clientEvent,
      boolean returnTombstones, boolean retainResult, boolean createIfAbsent) {
    // We never have a local value if we are a stub...
    return null;
  }

  @Override
  public Entry<?, ?> getEntry(KeyInfo keyInfo, LocalRegion r, boolean allowTombstones) {
    return getTXRegionStub(r).getEntry(keyInfo, allowTombstones);
  }

  @Override
  public TXEvent getEvent() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<EntryEvent<?, ?>> getEvents() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<InternalRegion> getRegions() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TransactionId getTransactionId() {
    return proxy.getTxId();
  }

  @Override
  public void invalidateExistingEntry(EntryEventImpl event, boolean invokeCallbacks,
      boolean forceNewEntry) {
    if (event.getOperation().isLocal()) {
      throw new UnsupportedOperationInTransactionException(
          "localInvalidate() is not allowed in a transaction");
    }
    getTXRegionStub(event.getRegion()).invalidateExistingEntry(event, invokeCallbacks,
        forceNewEntry);

  }

  @Override
  public boolean isInProgress() {
    return proxy.isInProgress();
  }

  @Override
  public boolean isInProgressAndSameAs(TXStateInterface state) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean needsLargeModCount() {
    return false;
  }

  @Override
  public int nextModSerialNum() {
    return 0;
  }

  @Override
  public TXRegionState readRegion(InternalRegion r) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void rmRegion(LocalRegion r) {
    throw new UnsupportedOperationException();
  }

  public void setAfterSendRollback(Runnable afterSend) {
    internalAfterSendRollback = afterSend;
  }

  public void setAfterSendCommit(Runnable afterSend) {
    internalAfterSendCommit = afterSend;
  }

  @Override
  public boolean txPutEntry(EntryEventImpl event, boolean ifNew, boolean requireOldValue,
      boolean checkResources, Object expectedOldValue) {
    return false;
  }

  @Override
  public TXEntryState txReadEntry(KeyInfo entryKey, LocalRegion localRegion, boolean rememberRead,
      boolean createTxEntryIfAbsent) {
    return null;
  }

  @Override
  public TXRegionState txReadRegion(InternalRegion internalRegion) {
    return null;
  }

  @Override
  public TXRegionState txWriteRegion(InternalRegion internalRegion, KeyInfo entryKey) {
    return null;
  }

  @Override
  public TXRegionState writeRegion(InternalRegion r) {
    return null;
  }

  @Override
  public boolean containsKey(KeyInfo keyInfo, LocalRegion localRegion) {
    return getTXRegionStub(localRegion).containsKey(keyInfo);
  }

  @Override
  public boolean containsValueForKey(KeyInfo keyInfo, LocalRegion localRegion) {
    return getTXRegionStub(localRegion).containsValueForKey(keyInfo);
  }

  @Override
  public int entryCount(LocalRegion localRegion) {
    return getTXRegionStub(localRegion).entryCount();
  }

  @Override
  public Object findObject(KeyInfo keyInfo, LocalRegion r, boolean isCreate,
      boolean generateCallbacks, Object value, boolean disableCopyOnRead, boolean preferCD,
      ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent,
      boolean returnTombstones) {
    return getTXRegionStub(r).findObject(keyInfo, isCreate, generateCallbacks, value, preferCD,
        requestingClient, clientEvent);
  }

  @Override
  public Set<?> getAdditionalKeysForIterator(LocalRegion currRgn) {
    return null;
  }

  @Override
  public Object getEntryForIterator(KeyInfo keyInfo, LocalRegion currRgn, boolean rememberReads,
      boolean allowTombstones) {
    return getTXRegionStub(currRgn).getEntryForIterator(keyInfo, allowTombstones);
  }

  @Override
  public Object getKeyForIterator(KeyInfo keyInfo, LocalRegion currRgn, boolean rememberReads,
      boolean allowTombstones) {
    Object key = keyInfo.getKey();
    if (key instanceof RegionEntry) {
      return ((RegionEntry) key).getKey();
    }
    return key;
  }

  @Override
  public Object getValueInVM(KeyInfo keyInfo, LocalRegion localRegion, boolean rememberRead) {
    return null;
  }

  @Override
  public boolean isDeferredStats() {
    return true;
  }

  @Override
  public boolean putEntry(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed) {
    return putEntry(event, ifNew, ifOld, expectedOldValue, requireOldValue, lastModified,
        overwriteDestroyed, true,
        false);
  }

  @Override
  public boolean putEntry(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed, boolean invokeCallbacks, boolean throwConcurrentModification) {
    return getTXRegionStub(event.getRegion()).putEntry(event, ifNew, ifOld, expectedOldValue,
        requireOldValue, lastModified, overwriteDestroyed);
  }

  @Override
  public Object getSerializedValue(LocalRegion localRegion, KeyInfo key, boolean doNotLockEntry,
      ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent,
      boolean returnTombstones) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean putEntryOnRemote(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed) throws DataLocationException {
    throw new IllegalStateException();
  }

  @Override
  public boolean isFireCallbacks() {
    return false;
  }

  @Override
  public void destroyOnRemote(EntryEventImpl event, boolean cacheWrite, Object expectedOldValue)
      throws DataLocationException {
    throw new IllegalStateException();
  }

  @Override
  public void invalidateOnRemote(EntryEventImpl event, boolean invokeCallbacks,
      boolean forceNewEntry) throws DataLocationException {
    throw new IllegalStateException();
  }

  @Override
  public void checkSupportsRegionDestroy() throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        "destroyRegion() is not supported while in a transaction");
  }

  @Override
  public void checkSupportsRegionInvalidate() throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        "invalidateRegion() is not supported while in a transaction");
  }

  @Override
  public void checkSupportsRegionClear() throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        "clear() is not supported while in a transaction");
  }

  @Override
  public Set<?> getBucketKeys(LocalRegion localRegion, BucketId bucketId, boolean allowTombstones) {
    PartitionedRegion pr = (PartitionedRegion) localRegion;
    /*
     * txtodo: what does this mean for c/s
     */
    return pr.getBucketKeys(bucketId, allowTombstones);
  }

  @Override
  public Entry<?, ?> getEntryOnRemote(KeyInfo key, LocalRegion localRegion, boolean allowTombstones)
      throws DataLocationException {
    throw new IllegalStateException();
  }

  @Override
  public ReentrantLock getLock() {
    return proxy.getLock();
  }

  @Override
  public Set<?> getRegionKeysForIteration(LocalRegion currRegion) {
    return getTXRegionStub(currRegion).getRegionKeysForIteration();
  }

  @Override
  public boolean isRealDealLocal() {
    return false;
  }

  public DistributedMember getTarget() {
    return target;
  }

  @Override
  public void postPutAll(DistributedPutAllOperation putallOp, VersionedObjectList successfulPuts,
      InternalRegion reg) {
    getTXRegionStub(reg).postPutAll(putallOp, successfulPuts, reg);
  }

  @Override
  public void postRemoveAll(DistributedRemoveAllOperation op, VersionedObjectList successfulOps,
      InternalRegion reg) {
    getTXRegionStub(reg).postRemoveAll(op, successfulOps, reg);
  }

  @Override
  public Entry<?, ?> accessEntry(KeyInfo keyInfo, LocalRegion localRegion) {
    return getEntry(keyInfo, localRegion, false);
  }

  @Override
  public void updateEntryVersion(EntryEventImpl event) throws EntryNotFoundException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {}

  @Override
  public boolean isTxState() {
    return false;
  }

  @Override
  public boolean isTxStateStub() {
    return true;
  }

  @Override
  public boolean isTxStateProxy() {
    return false;
  }

  @Override
  public boolean isDistTx() {
    return false;
  }

  @Override
  public boolean isCreatedOnDistTxCoordinator() {
    return false;
  }
}
