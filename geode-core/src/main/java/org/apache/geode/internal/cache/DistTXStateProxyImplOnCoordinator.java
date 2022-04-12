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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.TransactionInDoubtException;
import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.TXEntryState.DistTxThinEntryState;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.cache.tx.DistClientTXStateStub;
import org.apache.geode.internal.statistics.StatisticsClock;

public class DistTXStateProxyImplOnCoordinator extends DistTXStateProxyImpl {

  /**
   * A map of distributed system member to either {@link DistPeerTXStateStub} or
   * {@link DistTXStateOnCoordinator} (in case of TX coordinator is also a data node)
   */
  private final HashMap<DistributedMember, DistTXCoordinatorInterface> target2realDeals =
      new HashMap<>();

  private HashMap<InternalRegion, DistributedMember> rrTargets;

  private Set<DistributedMember> txRemoteParticpants = null; // other than local

  private HashMap<String, ArrayList<DistTxThinEntryState>> txEntryEventMap = null;

  public DistTXStateProxyImplOnCoordinator(InternalCache cache, TXManagerImpl managerImpl, TXId id,
      InternalDistributedMember clientMember, StatisticsClock statisticsClock) {
    super(cache, managerImpl, id, clientMember, statisticsClock);
  }

  public DistTXStateProxyImplOnCoordinator(InternalCache cache, TXManagerImpl managerImpl, TXId id,
      boolean isjta, StatisticsClock statisticsClock) {
    super(cache, managerImpl, id, isjta, statisticsClock);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#commit()
   *
   * [DISTTX] TODO Catch all exceptions in precommit and rollback and make sure these messages reach
   * all
   */
  @Override
  public void commit() throws CommitConflictException {
    var preserveTx = false;
    var precommitResult = false;
    try {
      // create a map of secondary(for PR) / replica(for RR) to stubs to send
      // commit message to those
      var otherTargets2realDeals =
          getSecondariesAndReplicasForTxOps();
      // add it to the existing map and then send commit to all copies
      target2realDeals.putAll(otherTargets2realDeals);
      if (logger.isDebugEnabled()) {
        logger.debug(
            "DistTXStateProxyImplOnCoordinator.commit target2realDeals = " + target2realDeals);
      }

      precommitResult = doPrecommit();
      if (precommitResult) {
        if (logger.isDebugEnabled()) {
          logger.debug("DistTXStateProxyImplOnCoordinator.commit Going for commit ");
        }
        var phase2commitDone = doCommit();
        if (logger.isDebugEnabled()) {
          logger.debug("DistTXStateProxyImplOnCoordinator.commit Commit "
              + (phase2commitDone ? "Done" : "Failed"));
        }
        if (!phase2commitDone) {
          throw new TransactionInDoubtException(
              "Commit failed on cache server");
        }
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "DistTXStateProxyImplOnCoordinator.commit precommitResult = " + precommitResult);
        }
      }
    } catch (UnsupportedOperationInTransactionException e) {
      // fix for #42490
      preserveTx = true;
      throw e;
    } finally {
      if (!precommitResult) {
        rollback();
      }

      inProgress = preserveTx;
    }
  }

  /**
   * creates a map of all secondaries(for PR) / replicas(for RR) to stubs to send commit message to
   * those
   */
  private HashMap<DistributedMember, DistTXCoordinatorInterface> getSecondariesAndReplicasForTxOps() {
    var currentNode =
        getCache().getInternalDistributedSystem().getDistributedMember();

    var secondaryTarget2realDeals =
        new HashMap<DistributedMember, DistTXCoordinatorInterface>();
    for (var e : target2realDeals.entrySet()) {
      var originalTarget = e.getKey();
      var distPeerTxStateStub = e.getValue();

      var primaryTxOps =
          distPeerTxStateStub.getPrimaryTransactionalOperations();
      for (var dtop : primaryTxOps) {
        var internalRegion = dtop.getRegion();
        // replicas or secondaries
        Set<InternalDistributedMember> otherNodes = null;
        if (internalRegion instanceof PartitionedRegion) {
          var allNodes = ((PartitionedRegion) dtop.getRegion())
              .getRegionAdvisor().getBucketOwners(dtop.getKeyInfo().getBucketId());
          allNodes.remove(originalTarget);
          otherNodes = allNodes;
        } else if (internalRegion instanceof DistributedRegion) {
          otherNodes = ((DistributedRegion) internalRegion).getCacheDistributionAdvisor()
              .adviseInitializedReplicates();
          otherNodes.remove(originalTarget);
        }

        if (otherNodes != null) {
          for (var dm : otherNodes) {
            // whether the target already exists due to other Tx op on the node
            var existingDistPeerTXStateStub = target2realDeals.get(dm);
            if (existingDistPeerTXStateStub == null) {
              existingDistPeerTXStateStub = secondaryTarget2realDeals.get(dm);
              if (existingDistPeerTXStateStub == null) {
                DistTXCoordinatorInterface newTxStub = null;
                if (currentNode.equals(dm)) {
                  // [DISTTX] TODO add a test case for this condition?
                  newTxStub = new DistTXStateOnCoordinator(this, false, getStatisticsClock());
                } else {
                  newTxStub = new DistPeerTXStateStub(this, dm, onBehalfOfClientMember);
                }
                newTxStub.addSecondaryTransactionalOperations(dtop);
                secondaryTarget2realDeals.put(dm, newTxStub);
              } else {
                existingDistPeerTXStateStub.addSecondaryTransactionalOperations(dtop);
              }
            } else {
              existingDistPeerTXStateStub.addSecondaryTransactionalOperations(dtop);
            }
          }
        }
      }
    }
    return secondaryTarget2realDeals;
  }

  @Override
  public void rollback() {
    if (logger.isDebugEnabled()) {
      logger.debug("DistTXStateProxyImplOnCoordinator.rollback Going for rollback ");
    }

    var finalResult = false;
    final var dm = getCache().getDistributionManager();
    try {
      // Create Tx Participants
      var txRemoteParticpants = getTxRemoteParticpants(dm);

      // create processor and rollback message
      var processor =
          new DistTXRollbackMessage.DistTxRollbackReplyProcessor(getTxId(), dm,
              txRemoteParticpants, target2realDeals);
      // TODO [DISTTX} whats ack threshold?
      processor.enableSevereAlertProcessing();
      final var rollbackMsg =
          new DistTXRollbackMessage(getTxId(), onBehalfOfClientMember, processor);

      // send rollback message to remote nodes
      for (var remoteNode : txRemoteParticpants) {
        var remoteTXStateStub = target2realDeals.get(remoteNode);
        if (remoteTXStateStub.isTxState()) {
          throw new UnsupportedOperationInTransactionException(
              String.format("Expected %s during a distributed transaction but got %s",
                  "DistPeerTXStateStub",
                  remoteTXStateStub.getClass().getSimpleName()));
        }
        try {
          remoteTXStateStub.setRollbackMessage(rollbackMsg, dm);
          remoteTXStateStub.rollback();
        } finally {
          remoteTXStateStub.setRollbackMessage(null, null);
          remoteTXStateStub.finalCleanup();
        }
        if (logger.isDebugEnabled()) { // TODO - make this trace level
          logger.debug("DistTXStateProxyImplOnCoordinator.rollback target = " + remoteNode);
        }
      }

      // Do rollback on local node
      var localTXState = target2realDeals.get(dm.getId());
      if (localTXState != null) {
        if (!localTXState.isTxState()) {
          throw new UnsupportedOperationInTransactionException(
              String.format("Expected %s during a distributed transaction but got %s",
                  "DistTXStateOnCoordinator",
                  localTXState.getClass().getSimpleName()));
        }
        localTXState.rollback();
        var localResult = localTXState.getRollbackResponse();
        if (logger.isDebugEnabled()) {
          logger.debug("DistTXStateProxyImplOnCoordinator.rollback local = " + dm.getId()
              + " ,result= " + localResult + " ,finalResult-old= " + finalResult);
        }
        finalResult = finalResult && localResult;
      }

      /*
       * [DISTTX] TODO Any test hooks
       */
      // if (internalAfterIndividualSend != null) {
      // internalAfterIndividualSend.run();
      // }

      /*
       * [DISTTX] TODO see how to handle exception
       */

      /*
       * [DISTTX] TODO Any test hooks
       */
      // if (internalAfterIndividualCommitProcess != null) {
      // // Testing callback
      // internalAfterIndividualCommitProcess.run();
      // }

      { // Wait for results
        dm.getCancelCriterion().checkCancelInProgress(null);
        processor.waitForPrecommitCompletion();

        // [DISTTX} TODO Handle stats
        // dm.getStats().incCommitWaits();

        var remoteResults = processor.getRollbackResponseMap();
        for (var e : remoteResults.entrySet()) {
          var target = e.getKey();
          var remoteResult = e.getValue();
          if (logger.isDebugEnabled()) { // TODO - make this trace level
            logger.debug("DistTXStateProxyImplOnCoordinator.rollback target = " + target
                + " ,result= " + remoteResult + " ,finalResult-old= " + finalResult);
          }
          finalResult = finalResult && remoteResult;
        }
      }

    } finally {
      inProgress = false;
    }

    /*
     * [DISTTX] TODO Write similar method to take out exception
     *
     * [DISTTX] TODO Handle Reliable regions
     */
    // if (this.hasReliableRegions) {
    // checkDistributionReliability(distMap, processor);
    // }

    if (logger.isDebugEnabled()) {
      logger.debug("DistTXStateProxyImplOnCoordinator.rollback finalResult= " + finalResult);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TXStateInterface getRealDeal(KeyInfo key, InternalRegion r) {
    if (r != null) {
      target = null;
      // wait for the region to be initialized fixes bug 44652
      r.waitOnInitialization(r.getInitializationLatchBeforeGetInitialImage());
      if (r instanceof PartitionedRegion) {
        target = getOwnerForKey(r, key);
      } else if (r instanceof BucketRegion) {
        target = ((BucketRegion) r).getBucketAdvisor().getPrimary();
        // target = r.getMyId();
      } else { // replicated region
        target = getRRTarget(key, r);
      }
      realDeal = target2realDeals.get(target);
    }
    if (realDeal == null) {
      // assert (r != null);
      if (r == null) { // TODO: stop gap to get tests working
        realDeal = new DistTXStateOnCoordinator(this, false, getStatisticsClock());
        target = txMgr.getDM().getId();
      } else {
        // Code to keep going forward
        if (r.hasServerProxy()) {
          // TODO [DISTTX] See what we need for client?
          realDeal =
              new DistClientTXStateStub(r.getCache(), r.getDistributionManager(), this, target, r);
          if (r.getScope().isDistributed()) {
            if (txDistributedClientWarningIssued.compareAndSet(false, true)) {
              logger.warn(
                  "Distributed region {} is being used in a client-initiated transaction.  The transaction will only affect servers and this client.  To keep from seeing this message use 'local' scope in client regions used in transactions.",
                  r.getFullPath());
            }
          }
        } else {
          // (r != null) code block above
          if (target == null || target.equals(txMgr.getDM().getId())) {
            realDeal = new DistTXStateOnCoordinator(this, false, getStatisticsClock());
          } else {
            realDeal = new DistPeerTXStateStub(this, target, onBehalfOfClientMember);
          }
        }
      }
      if (logger.isDebugEnabled()) {
        logger.debug(
            "DistTXStateProxyImplOnCoordinator::getRealDeal Built a new TXState: {} txMge:{} proxy {} target {}",
            realDeal, txMgr.getDM().getId(), this, target/* , new Throwable() */);
      }
      target2realDeals.put(target, (DistTXCoordinatorInterface) realDeal);
      if (logger.isDebugEnabled()) {
        logger
            .debug("DistTXStateProxyImplOnCoordinator.getRealDeal added TxState target2realDeals = "
                + target2realDeals);
      }
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "DistTXStateProxyImplOnCoordinator::getRealDeal Found TXState: {} proxy {} target {} target2realDeals {}",
            realDeal, this, target, target2realDeals);
      }
    }
    return realDeal;
  }

  @Override
  public TXStateInterface getRealDeal(DistributedMember t) {
    assert t != null;
    realDeal = target2realDeals.get(target);
    if (realDeal == null) {
      target = t;
      realDeal = new DistPeerTXStateStub(this, target, onBehalfOfClientMember);
      if (logger.isDebugEnabled()) {
        logger.debug(
            "DistTXStateProxyImplOnCoordinator::getRealDeal(t) Built a new TXState: {} me:{}",
            realDeal, txMgr.getDM().getId());
      }
      if (!realDeal.isDistTx() || realDeal.isCreatedOnDistTxCoordinator()
          || !realDeal.isTxState()) {
        throw new UnsupportedOperationInTransactionException(
            String.format("Expected %s during a distributed transaction but got %s",
                "DistPeerTXStateStub", realDeal.getClass().getSimpleName()));
      }
      target2realDeals.put(target, (DistPeerTXStateStub) realDeal);
      if (logger.isDebugEnabled()) {
        logger.debug(
            "DistTXStateProxyImplOnCoordinator.getRealDeal(t) added TxState target2realDeals = "
                + target2realDeals);
      }
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "DistTXStateProxyImplOnCoordinator::getRealDeal(t) Found TXState: {} proxy {} target {} target2realDeals {}",
            realDeal, this, target, target2realDeals);
      }
    }
    return realDeal;
  }

  /*
   * [DISTTX] TODO Do some optimization
   */
  private DistributedMember getRRTarget(KeyInfo key, InternalRegion r) {
    if (rrTargets == null) {
      rrTargets = new HashMap();
    }
    var m = rrTargets.get(r);
    if (m == null) {
      m = getOwnerForKey(r, key);
      rrTargets.put(r, m);
    }
    return m;
  }

  private Set<DistributedMember> getTxRemoteParticpants(final DistributionManager dm) {
    if (txRemoteParticpants == null) {
      var txParticpants = target2realDeals.keySet();
      txRemoteParticpants = new HashSet<>(txParticpants);
      // Remove local member from remote participant list
      txRemoteParticpants.remove(dm.getId());
      if (logger.isDebugEnabled()) {
        logger.debug("DistTXStateProxyImplOnCoordinator.doPrecommit txParticpants = "
            + txParticpants + " ,txRemoteParticpants=" + txRemoteParticpants + " ,originator="
            + dm.getId());
      }
    }
    return txRemoteParticpants;
  }

  private boolean doPrecommit() {
    var finalResult = true;
    final var dm = getCache().getDistributionManager();
    var txRemoteParticpants = getTxRemoteParticpants(dm);

    // create processor and precommit message
    var processor =
        new DistTXPrecommitMessage.DistTxPrecommitReplyProcessor(getTxId(), dm,
            txRemoteParticpants, target2realDeals);
    // TODO [DISTTX} whats ack threshold?
    processor.enableSevereAlertProcessing();
    final var precommitMsg =
        new DistTXPrecommitMessage(getTxId(), onBehalfOfClientMember, processor);

    // send precommit message to remote nodes
    for (var remoteNode : txRemoteParticpants) {
      var remoteTXStateStub = target2realDeals.get(remoteNode);
      if (remoteTXStateStub.isTxState()) {
        throw new UnsupportedOperationInTransactionException(
            String.format("Expected %s during a distributed transaction but got %s",
                "DistPeerTXStateStub",
                remoteTXStateStub.getClass().getSimpleName()));
      }
      try {
        remoteTXStateStub.setPrecommitMessage(precommitMsg, dm);
        remoteTXStateStub.precommit();
      } finally {
        remoteTXStateStub.setPrecommitMessage(null, null);
      }
      if (logger.isDebugEnabled()) {
        logger.debug(
            "DistTXStateProxyImplOnCoordinator.doPrecommit Sent Message to target = " + remoteNode);
      }
    }

    // Do precommit on local node
    var sortedRegionName = new TreeSet<String>();
    var localTXState = target2realDeals.get(dm.getId());
    if (localTXState != null) {
      if (!localTXState.isTxState()) {
        throw new UnsupportedOperationInTransactionException(
            String.format("Expected %s during a distributed transaction but got %s",
                "DistTXStateOnCoordinator",
                localTXState.getClass().getSimpleName()));
      }
      localTXState.precommit();
      var localResult = localTXState.getPreCommitResponse();
      var entryStateSortedMap =
          new TreeMap<String, ArrayList<DistTxThinEntryState>>();
      ArrayList<ArrayList<DistTxThinEntryState>> entryEventList = null;
      if (localResult) {
        localResult = ((DistTXStateOnCoordinator) localTXState)
            .populateDistTxEntryStateList(entryStateSortedMap);
        if (localResult) {
          entryEventList =
              new ArrayList<>(entryStateSortedMap.values());
          populateEntryEventMap(dm.getId(), entryEventList, sortedRegionName);
        }
      }

      if (logger.isDebugEnabled()) {
        logger.debug("DistTXStateProxyImplOnCoordinator.doPrecommit local = " + dm.getId()
            + " ,entryEventList=" + printEntryEventList(entryEventList) + " ,txRegionVersionsMap="
            + printEntryEventMap(txEntryEventMap) + " ,result= " + localResult
            + " ,finalResult-old= " + finalResult);
      }
      finalResult = finalResult && localResult;
    }

    /*
     * [DISTTX] TODO Any test hooks
     */
    // if (internalAfterIndividualSend != null) {
    // internalAfterIndividualSend.run();
    // }

    /*
     * [DISTTX] TODO see how to handle exception
     */

    /*
     * [DISTTX] TODO Any test hooks
     */
    // if (internalAfterIndividualCommitProcess != null) {
    // // Testing callback
    // internalAfterIndividualCommitProcess.run();
    // }

    { // Wait for results
      dm.getCancelCriterion().checkCancelInProgress(null);
      processor.waitForPrecommitCompletion();

      // [DISTTX} TODO Handle stats
      // dm.getStats().incCommitWaits();

      var remoteResults =
          processor.getCommitResponseMap();
      for (var e : remoteResults.entrySet()) {
        var target = e.getKey();
        var remoteResponse = e.getValue();
        var entryEventList =
            remoteResponse.getDistTxEntryEventList();
        populateEntryEventMap(target, entryEventList, sortedRegionName);
        if (logger.isDebugEnabled()) {
          logger.debug("DistTXStateProxyImplOnCoordinator.doPrecommit got reply from target = "
              + target + " ,sortedRegions" + sortedRegionName + " ,entryEventList="
              + printEntryEventList(entryEventList) + " ,txEntryEventMap="
              + printEntryEventMap(txEntryEventMap) + " ,result= "
              + remoteResponse.getCommitState() + " ,finalResult-old= " + finalResult);
        }
        finalResult = finalResult && remoteResponse.getCommitState();
      }
    }

    /*
     * [DISTTX] TODO Write similar method to take out exception
     *
     * [DISTTX] TODO Handle Reliable regions
     */
    // if (this.hasReliableRegions) {
    // checkDistributionReliability(distMap, processor);
    // }

    if (logger.isDebugEnabled()) {
      logger.debug("DistTXStateProxyImplOnCoordinator.doPrecommit finalResult= " + finalResult);
    }
    return finalResult;
  }

  /*
   * Handle response of precommit reply
   *
   * Go over list of region versions for this target and fill map
   */
  private void populateEntryEventMap(DistributedMember target,
      ArrayList<ArrayList<DistTxThinEntryState>> entryEventList, TreeSet<String> sortedRegionName) {
    if (txEntryEventMap == null) {
      txEntryEventMap = new HashMap<>();
    }

    var distTxIface = target2realDeals.get(target);
    if (distTxIface.getPrimaryTransactionalOperations() != null
        && distTxIface.getPrimaryTransactionalOperations().size() > 0) {
      sortedRegionName.clear();
      distTxIface.gatherAffectedRegionsName(sortedRegionName, true, false);

      if (sortedRegionName.size() != entryEventList.size()) {
        throw new UnsupportedOperationInTransactionException(
            String.format("Expected %s during a distributed transaction but got %s",
                "size of " + sortedRegionName.size() + " {" + sortedRegionName + "}"
                    + " for target=" + target,
                entryEventList.size() + " {" + entryEventList + "}"));
      }

      var index = 0;
      // Get region as per sorted order of region path
      for (var rName : sortedRegionName) {
        txEntryEventMap.put(rName, entryEventList.get(index++));
      }
    }
  }

  /*
   * Populate list of regions for this target, while sending commit messages
   */
  private void populateEntryEventList(DistributedMember target,
      ArrayList<ArrayList<DistTxThinEntryState>> entryEventList, TreeSet<String> sortedRegionMap) {
    var distTxItem = target2realDeals.get(target);
    sortedRegionMap.clear();
    distTxItem.gatherAffectedRegionsName(sortedRegionMap, false, true);

    // Get region as per sorted order of region path
    entryEventList.clear();
    for (var rName : sortedRegionMap) {
      var entryStates = txEntryEventMap.get(rName);
      if (entryStates == null) {
        throw new UnsupportedOperationInTransactionException(
            String.format("Expected %s during a distributed transaction but got %s",
                "entryStates for " + rName + " at target " + target, "null"));
      }
      entryEventList.add(entryStates);
    }
  }

  /*
   * [DISTTX] TODO - Handle result TXMessage
   */
  private boolean doCommit() {
    var finalResult = true;
    final var dm = getCache().getDistributionManager();

    // Create Tx Participants
    var txRemoteParticpants = getTxRemoteParticpants(dm);

    // create processor and commit message
    var processor =
        new DistTXCommitMessage.DistTxCommitReplyProcessor(getTxId(), dm, txRemoteParticpants,
            target2realDeals);
    // TODO [DISTTX} whats ack threshold?
    processor.enableSevereAlertProcessing();
    final var commitMsg =
        new DistTXCommitMessage(getTxId(), onBehalfOfClientMember, processor);

    // send commit message to remote nodes
    var entryEventList = new ArrayList<ArrayList<DistTxThinEntryState>>();
    var sortedRegionName = new TreeSet<String>();
    for (var remoteNode : txRemoteParticpants) {
      var remoteTXStateStub = target2realDeals.get(remoteNode);
      if (remoteTXStateStub.isTxState()) {
        throw new UnsupportedOperationInTransactionException(
            String.format("Expected %s during a distributed transaction but got %s",
                "DistPeerTXStateStub",
                remoteTXStateStub.getClass().getSimpleName()));
      }
      try {
        populateEntryEventList(remoteNode, entryEventList, sortedRegionName);
        commitMsg.setEntryStateList(entryEventList);
        remoteTXStateStub.setCommitMessage(commitMsg, dm);
        remoteTXStateStub.commit();
      } finally {
        remoteTXStateStub.setCommitMessage(null, null);
        remoteTXStateStub.finalCleanup();
      }
      if (logger.isDebugEnabled()) {
        logger.debug("DistTXStateProxyImplOnCoordinator.doCommit Sent Message target = "
            + remoteNode + " ,sortedRegions=" + sortedRegionName + " ,entryEventList="
            + printEntryEventList(entryEventList) + " ,txEntryEventMap="
            + printEntryEventMap(txEntryEventMap));
      }
    }

    // Do commit on local node
    var localTXState = target2realDeals.get(dm.getId());
    if (localTXState != null) {
      if (!localTXState.isTxState()) {
        throw new UnsupportedOperationInTransactionException(
            String.format("Expected %s during a distributed transaction but got %s",
                "DistTXStateOnCoordinator",
                localTXState.getClass().getSimpleName()));
      }
      populateEntryEventList(dm.getId(), entryEventList, sortedRegionName);
      ((DistTXStateOnCoordinator) localTXState).setDistTxEntryStates(entryEventList);
      localTXState.commit();
      var localResultMsg = localTXState.getCommitMessage();
      if (logger.isDebugEnabled()) {
        logger.debug(
            "DistTXStateProxyImplOnCoordinator.doCommit local = " + dm.getId() + " ,sortedRegions="
                + sortedRegionName + " ,entryEventList=" + printEntryEventList(entryEventList)
                + " ,txEntryEventMap=" + printEntryEventMap(txEntryEventMap) + " ,result= "
                + (localResultMsg != null) + " ,finalResult-old= " + finalResult);
      }
      finalResult = finalResult && (localResultMsg != null);
    }

    /*
     * [DISTTX] TODO Any test hooks
     */
    // if (internalAfterIndividualSend != null) {
    // internalAfterIndividualSend.run();
    // }

    /*
     * [DISTTX] TODO see how to handle exception
     */

    /*
     * [DISTTX] TODO Any test hooks
     */
    // if (internalAfterIndividualCommitProcess != null) {
    // // Testing callback
    // internalAfterIndividualCommitProcess.run();
    // }

    { // Wait for results
      dm.getCancelCriterion().checkCancelInProgress(null);
      processor.waitForPrecommitCompletion();

      // [DISTTX} TODO Handle stats
      dm.getStats().incCommitWaits();

      var remoteResults = processor.getCommitResponseMap();
      for (var e : remoteResults.entrySet()) {
        var target = e.getKey();
        var remoteResultMsg = e.getValue();
        if (logger.isDebugEnabled()) { // TODO - make this trace level
          logger.debug(
              "DistTXStateProxyImplOnCoordinator.doCommit got results from target = " + target
                  + " ,result= " + (remoteResultMsg != null) + " ,finalResult-old= " + finalResult);
        }
        finalResult = finalResult && remoteResultMsg != null;
      }
    }

    /*
     * [DISTTX] TODO Write similar method to take out exception
     *
     * [DISTTX] TODO Handle Reliable regions
     */
    // if (this.hasReliableRegions) {
    // checkDistributionReliability(distMap, processor);
    // }

    if (logger.isDebugEnabled()) {
      logger.debug("DistTXStateProxyImplOnCoordinator.doCommit finalResult= " + finalResult);
    }
    return finalResult;
  }

  /**
   * For distributed transactions, this divides the user's putAll operation into multiple per bucket
   * putAll ops(with entries to be put in that bucket) and then fires those using using appropriate
   * TXStateStub (for target that host the corresponding bucket)
   */
  @Override
  public void postPutAll(DistributedPutAllOperation putallOp, VersionedObjectList successfulPuts,
      InternalRegion reg) {
    if (putallOp.putAllData.length == 0) {
      return;
    }
    if (reg instanceof DistributedRegion) {
      super.postPutAll(putallOp, successfulPuts, reg);
    } else {
      reg.getCancelCriterion().checkCancelInProgress(null); // fix for bug
      // #43651

      if (logger.isDebugEnabled()) {
        logger.debug(
            "DistTXStateProxyImplOnCoordinator.postPutAll "
                + "processing putAll op for region {}, size of putAllOp " + "is {}",
            reg, putallOp.putAllData.length);
      }


      // map of bucketId to putall op for this bucket
      var bucketToPutallMap =
          new HashMap<Integer, DistributedPutAllOperation>();
      // map of bucketId to TXStateStub for target that hosts this bucket
      var bucketToTxStateStubMap =
          new HashMap<Integer, DistTXCoordinatorInterface>();

      // separate the putall op per bucket
      for (var i = 0; i < putallOp.putAllData.length; i++) {
        assert (putallOp.putAllData[i] != null);
        var key = putallOp.putAllData[i].key;
        int bucketId = putallOp.putAllData[i].getBucketId();

        var putAllForBucket = bucketToPutallMap.get(bucketId);
        if (putAllForBucket == null) {
          // TODO DISTTX: event is never released
          var event = EntryEventImpl.createPutAllEvent(null, reg,
              Operation.PUTALL_CREATE, key, putallOp.putAllData[i].getValue(reg.getCache()));
          event.setEventId(putallOp.putAllData[i].getEventID());
          putAllForBucket =
              new DistributedPutAllOperation(event, putallOp.putAllDataSize, putallOp.isBridgeOp);
          bucketToPutallMap.put(bucketId, putAllForBucket);
        }
        putallOp.putAllData[i].setFakeEventID();
        putAllForBucket.addEntry(putallOp.putAllData[i]);

        var ki = new KeyInfo(key, null, null);
        var tsi = (DistTXCoordinatorInterface) getRealDeal(ki, reg);
        bucketToTxStateStubMap.put(bucketId, tsi);
      }

      // fire a putAll operation for each bucket using appropriate TXStateStub
      // (for target that host this bucket)

      // [DISTTX] [TODO] Perf: Can this be further optimized?
      // This sends putAll in a loop to each target bucket (and waits for ack)
      // one after another.Could we send respective putAll messages to all
      // targets using same reply processor and wait on it?
      for (var e : bucketToTxStateStubMap.entrySet()) {
        var bucketId = e.getKey();
        var dtsi = e.getValue();
        var putAllForBucket = bucketToPutallMap.get(bucketId);

        if (logger.isDebugEnabled()) {
          logger.debug(
              "DistTXStateProxyImplOnCoordinator.postPutAll processing"
                  + " putAll for ##bucketId = {}, ##txStateStub = {}, " + "##putAllOp = {}",
              bucketId, dtsi, putAllForBucket);
        }
        dtsi.postPutAll(putAllForBucket, successfulPuts, reg);
      }
    }
  }

  /**
   * For distributed transactions, this divides the user's removeAll operation into multiple per
   * bucket removeAll ops(with entries to be removed from that bucket) and then fires those using
   * using appropriate TXStateStub (for target that host the corresponding bucket)
   */
  @Override
  public void postRemoveAll(DistributedRemoveAllOperation op, VersionedObjectList successfulOps,
      InternalRegion reg) {
    if (op.removeAllData.length == 0) {
      return;
    }
    if (reg instanceof DistributedRegion) {
      super.postRemoveAll(op, successfulOps, reg);
    } else {
      reg.getCancelCriterion().checkCancelInProgress(null); // fix for bug
      // #43651
      if (logger.isDebugEnabled()) {
        logger.debug(
            "DistTXStateProxyImplOnCoordinator.postRemoveAll "
                + "processing removeAll op for region {}, size of removeAll " + "is {}",
            reg, op.removeAllDataSize);
      }

      // map of bucketId to removeAll op for this bucket
      var bucketToRemoveAllMap =
          new HashMap<Integer, DistributedRemoveAllOperation>();
      // map of bucketId to TXStateStub for target that hosts this bucket
      var bucketToTxStateStubMap =
          new HashMap<Integer, DistTXCoordinatorInterface>();

      // separate the removeAll op per bucket
      for (var i = 0; i < op.removeAllData.length; i++) {
        assert (op.removeAllData[i] != null);
        var key = op.removeAllData[i].key;
        int bucketId = op.removeAllData[i].getBucketId();

        var removeAllForBucket = bucketToRemoveAllMap.get(bucketId);
        if (removeAllForBucket == null) {
          // TODO DISTTX: event is never released
          var event = EntryEventImpl.createRemoveAllEvent(op, reg, key);
          event.setEventId(op.removeAllData[i].getEventID());
          removeAllForBucket =
              new DistributedRemoveAllOperation(event, op.removeAllDataSize, op.isBridgeOp);
          bucketToRemoveAllMap.put(bucketId, removeAllForBucket);
        }
        op.removeAllData[i].setFakeEventID();
        removeAllForBucket.addEntry(op.removeAllData[i]);

        var ki = new KeyInfo(key, null, null);
        var tsi = (DistTXCoordinatorInterface) getRealDeal(ki, reg);
        bucketToTxStateStubMap.put(bucketId, tsi);
      }

      // fire a removeAll operation for each bucket using appropriate TXStateStub
      // (for target that host this bucket)

      // [DISTTX] [TODO] Perf: Can this be further optimized?
      // This sends putAll in a loop to each target bucket (and waits for ack)
      // one after another.Could we send respective putAll messages to all
      // targets using same reply processor and wait on it?
      for (var e : bucketToTxStateStubMap.entrySet()) {
        var bucketId = e.getKey();
        var dtsi = e.getValue();
        var removeAllForBucket = bucketToRemoveAllMap.get(bucketId);

        if (logger.isDebugEnabled()) {
          logger.debug(
              "DistTXStateProxyImplOnCoordinator.postRemoveAll processing"
                  + " removeAll for ##bucketId = {}, ##txStateStub = {}, " + "##removeAllOp = {}",
              bucketId, dtsi, removeAllForBucket);
        }
        dtsi.postRemoveAll(removeAllForBucket, successfulOps, reg);
      }

    }
  }

  @Override
  public boolean isCreatedOnDistTxCoordinator() {
    return true;
  }

  public static String printEntryEventMap(
      HashMap<String, ArrayList<DistTxThinEntryState>> txRegionVersionsMap) {
    var str = new StringBuilder();
    str.append(" (");
    str.append(txRegionVersionsMap.size());
    str.append(")=[ ");
    for (var entry : txRegionVersionsMap
        .entrySet()) {
      str.append(" {").append(entry.getKey());
      str.append(":").append("size(").append(entry.getValue().size()).append(")");
      str.append("=").append(entry.getValue()).append("}, ");
    }
    str.append(" } ");
    return str.toString();
  }

  public static String printEntryEventList(
      ArrayList<ArrayList<DistTxThinEntryState>> entryEventList) {
    var str = new StringBuilder();
    str.append(" (");
    str.append(entryEventList.size());
    str.append(")=[ ");
    for (var entry : entryEventList) {
      str.append(" ( ");
      str.append(entry.size());
      str.append(" )={").append(entry);
      str.append(" } ");
    }
    str.append(" ] ");
    return str.toString();
  }

  /*
   * Do not return null
   */
  public DistributedMember getOwnerForKey(InternalRegion r, KeyInfo key) {
    var m = r.getOwnerForKey(key);
    if (m == null) {
      m = getCache().getDistributedSystem().getDistributedMember();
    }
    return m;
  }
}
