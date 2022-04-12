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
package org.apache.geode.internal.cache.tier.sockets;

import static java.util.Collections.emptySet;
import static org.apache.geode.internal.util.CollectionUtils.asSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;

import org.apache.geode.cache.Operation;
import org.apache.geode.internal.cache.Conflatable;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.FilterProfile;
import org.apache.geode.internal.cache.FilterRoutingInfo;
import org.apache.geode.internal.cache.FilterRoutingInfo.FilterInfo;
import org.apache.geode.internal.cache.InternalCacheEvent;
import org.apache.geode.internal.cache.InternalRegion;

public class ClientRegistrationEventQueueManagerTest {

  private CacheClientNotifier cacheClientNotifier;
  private CacheClientProxy cacheClientProxy;
  private ClientProxyMembershipID clientProxyMembershipId;
  private ClientUpdateMessageImpl clientUpdateMessage;
  private FilterInfo filterInfo;
  private FilterProfile filterProfile;
  private FilterRoutingInfo filterRoutingInfo;
  private InternalCacheEvent internalCacheEvent;
  private InternalRegion internalRegion;
  private Operation operation;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  @Before
  public void setUp() {
    cacheClientNotifier = mock(CacheClientNotifier.class);
    cacheClientProxy = mock(CacheClientProxy.class);
    clientProxyMembershipId = mock(ClientProxyMembershipID.class);
    clientUpdateMessage = mock(ClientUpdateMessageImpl.class);
    filterInfo = mock(FilterInfo.class);
    filterProfile = mock(FilterProfile.class);
    filterRoutingInfo = mock(FilterRoutingInfo.class);
    internalCacheEvent = mock(InternalCacheEvent.class);
    internalRegion = mock(InternalRegion.class);
    operation = mock(Operation.class);
  }

  @Test
  public void messageDeliveredAfterRegisteringOnDrainIfNewFilterIDsIncludesClient() {
    // this test requires mock of EntryEventImpl instead of InternalCacheEvent
    var entryEventImpl = mock(EntryEventImpl.class);

    when(cacheClientNotifier.getClientProxy(clientProxyMembershipId))
        .thenReturn(cacheClientProxy);
    when(cacheClientNotifier.getFilterClientIDs(entryEventImpl, filterProfile, filterInfo,
        clientUpdateMessage))
            .thenReturn(asSet(clientProxyMembershipId));
    when(cacheClientProxy.getProxyID())
        .thenReturn(clientProxyMembershipId);
    when(entryEventImpl.getOperation())
        .thenReturn(operation);
    when(entryEventImpl.getRegion())
        .thenReturn(internalRegion);
    when(filterProfile.getFilterRoutingInfoPart2(null, entryEventImpl))
        .thenReturn(filterRoutingInfo);
    when(filterRoutingInfo.getLocalFilterInfo())
        .thenReturn(filterInfo);
    when(internalRegion.getFilterProfile())
        .thenReturn(filterProfile);
    when(operation.isEntry())
        .thenReturn(true);

    var clientRegistrationEventQueueManager =
        new ClientRegistrationEventQueueManager();

    var clientRegistrationEventQueue =
        clientRegistrationEventQueueManager.create(clientProxyMembershipId,
            new ConcurrentLinkedQueue<>(), new ReentrantReadWriteLock());

    // Create empty filter client IDs produced by the "normal" put processing path, so we can test
    // that the event is still delivered if the client finished registering and needs the event.

    clientRegistrationEventQueueManager.add(entryEventImpl, clientUpdateMessage,
        clientUpdateMessage, emptySet(), cacheClientNotifier);

    clientRegistrationEventQueueManager.drain(clientRegistrationEventQueue, cacheClientNotifier);

    // The client update message should still be delivered because it is now part of the
    // filter clients interested in this event, despite having not been included in the original
    // filter info in the "normal" put processing path.
    verify(cacheClientProxy).deliverMessage(clientUpdateMessage);
  }

  @Test
  public void clientRemovedFromFilterClientsListIfEventAddedToRegistrationQueue() {
    // this test requires mock of EntryEventImpl instead of InternalCacheEvent
    var entryEventImpl = mock(EntryEventImpl.class);

    when(entryEventImpl.getOperation())
        .thenReturn(operation);
    when(operation.isEntry())
        .thenReturn(true);

    var clientRegistrationEventQueueManager =
        new ClientRegistrationEventQueueManager();

    clientRegistrationEventQueueManager.create(clientProxyMembershipId,
        new ConcurrentLinkedQueue<>(), new ReentrantReadWriteLock());

    // Add the registering client to the filter clients. This can happen if the filter info is
    // received but the client is not completely registered yet (queue GII has not been completed).
    // In that case, we want to remove the client from the filter IDs set and add the event
    // to the client's registration queue.
    var filterClientIds = asSet(clientProxyMembershipId);

    clientRegistrationEventQueueManager.add(entryEventImpl, mock(ClientUpdateMessageImpl.class),
        mock(Conflatable.class), filterClientIds, mock(CacheClientNotifier.class));

    // The client should no longer be in the filter clients since the event was queued in the
    // client's registration queue.
    assertThat(filterClientIds).isEmpty();
  }

  @Test
  public void putInProgressCounterIncrementedOnAddAndDecrementedOnRemoveForAllEvents() {
    var clientRegistrationEventQueueManager =
        new ClientRegistrationEventQueueManager();

    var clientRegistrationEventQueue =
        clientRegistrationEventQueueManager.create(mock(ClientProxyMembershipID.class),
            new ConcurrentLinkedQueue<>(), new ReentrantReadWriteLock());

    Collection<HAEventWrapper> haEventWrappers = new ArrayList<>();

    for (var i = 0; i < 5; ++i) {
      var entryEventImpl = mock(EntryEventImpl.class);

      when(entryEventImpl.getOperation())
          .thenReturn(operation);
      when(operation.isEntry())
          .thenReturn(true);

      var haEventWrapper = mock(HAEventWrapper.class);
      haEventWrappers.add(haEventWrapper);

      clientRegistrationEventQueueManager.add(entryEventImpl,
          mock(ClientUpdateMessageImpl.class), haEventWrapper, emptySet(), cacheClientNotifier);

      verify(haEventWrapper).incrementPutInProgressCounter(anyString());
    }

    clientRegistrationEventQueueManager.drain(clientRegistrationEventQueue, cacheClientNotifier);

    for (var haEventWrapper : haEventWrappers) {
      verify(haEventWrapper).decrementPutInProgressCounter();
    }
  }

  @Test
  public void addAndDrainQueueContentionTest() throws Exception {
    var readWriteLock = spy(new ReentrantReadWriteLock());

    when(readWriteLock.writeLock())
        .thenAnswer((Answer<WriteLock>) invocation -> {
          // Force a context switch from drain to put thread so we can ensure the event is not lost
          Thread.sleep(1);
          return (WriteLock) invocation.callRealMethod();
        });

    var clientRegistrationEventQueueManager =
        new ClientRegistrationEventQueueManager();

    var clientRegistrationEventQueue =
        clientRegistrationEventQueueManager.create(mock(ClientProxyMembershipID.class),
            new ConcurrentLinkedQueue<>(), readWriteLock);

    var addEventsToQueueTask = CompletableFuture.runAsync(() -> {
      for (var count = 0; count < 1_000; ++count) { // was 100_000
        // In thread one, we add events to the queue
        clientRegistrationEventQueueManager.add(entryEventImpl(),
            mock(ClientUpdateMessageImpl.class), mock(Conflatable.class), emptySet(),
            cacheClientNotifier);
      }
    });

    var drainEventsFromQueueTask = CompletableFuture.runAsync(() -> {
      // In thread two, we drain events from the queue
      clientRegistrationEventQueueManager.drain(clientRegistrationEventQueue,
          cacheClientNotifier);
    });

    CompletableFuture
        .allOf(addEventsToQueueTask, drainEventsFromQueueTask)
        .get();

    assertThat(clientRegistrationEventQueue.isEmpty()).isTrue();
  }

  @Test
  public void addEventWithOffheapValueCopiedToHeap() {
    // this test requires mock of EntryEventImpl instead of InternalCacheEvent
    var entryEventImpl = mock(EntryEventImpl.class);

    when(entryEventImpl.getOperation())
        .thenReturn(operation);
    when(operation.isEntry())
        .thenReturn(true);

    var clientRegistrationEventQueueManager =
        new ClientRegistrationEventQueueManager();

    clientRegistrationEventQueueManager.create(mock(ClientProxyMembershipID.class),
        new ConcurrentLinkedQueue<>(), new ReentrantReadWriteLock());

    clientRegistrationEventQueueManager.add(entryEventImpl, mock(ClientUpdateMessageImpl.class),
        mock(Conflatable.class), emptySet(), mock(CacheClientNotifier.class));

    verify(entryEventImpl).copyOffHeapToHeap();
  }

  @Test
  public void clientWasNeverRegisteredDrainQueueStillRemoved() {
    var clientRegistrationEventQueueManager =
        new ClientRegistrationEventQueueManager();

    var clientRegistrationEventQueue =
        clientRegistrationEventQueueManager.create(mock(ClientProxyMembershipID.class),
            new ConcurrentLinkedQueue<>(), new ReentrantReadWriteLock());

    clientRegistrationEventQueueManager.drain(clientRegistrationEventQueue, cacheClientNotifier);

    // Pass a new event to the ClientRegistrationEventQueueManager. This event should not be added
    // to the test client's registration queue, because it should already be removed. We can
    // validate that by asserting that the client's registration queue is empty after the add.
    clientRegistrationEventQueueManager.add(mock(InternalCacheEvent.class),
        mock(ClientUpdateMessageImpl.class), mock(Conflatable.class), emptySet(),
        cacheClientNotifier);

    assertThat(clientRegistrationEventQueue.isEmpty()).isTrue();
  }

  @Test
  public void drainThrowsExceptionQueueStillRemoved() {
    // this test requires mock of EntryEventImpl instead of InternalCacheEvent
    var entryEventImpl = mock(EntryEventImpl.class);
    var thrownException = new RuntimeException("thrownException");

    when(cacheClientNotifier.getClientProxy(clientProxyMembershipId))
        .thenReturn(mock(CacheClientProxy.class));
    when(entryEventImpl.getOperation())
        .thenReturn(operation);
    when(entryEventImpl.getRegion())
        .thenThrow(thrownException);
    when(operation.isEntry())
        .thenReturn(true);

    var clientRegistrationEventQueueManager =
        new ClientRegistrationEventQueueManager();

    var clientRegistrationEventQueue =
        clientRegistrationEventQueueManager.create(clientProxyMembershipId,
            new ConcurrentLinkedQueue<>(), new ReentrantReadWriteLock());

    Set<ClientProxyMembershipID> filterClientIds = new HashSet<>();

    clientRegistrationEventQueueManager.add(entryEventImpl, clientUpdateMessage,
        mock(Conflatable.class), filterClientIds, cacheClientNotifier);

    var thrown = catchThrowable(() -> {
      clientRegistrationEventQueueManager.drain(clientRegistrationEventQueue, cacheClientNotifier);
    });

    assertThat(thrown).isSameAs(thrownException);

    // Pass a new event to the ClientRegistrationEventQueueManager. This event should not be added
    // to the test client's registration queue, because it should already be removed. We can
    // validate that by asserting that the client's registration queue is empty after the add.
    clientRegistrationEventQueueManager.add(entryEventImpl, clientUpdateMessage,
        mock(Conflatable.class), filterClientIds, cacheClientNotifier);

    assertThat(clientRegistrationEventQueue.isEmpty())
        .withFailMessage(clientRegistrationEventQueue + " should be empty.")
        .isTrue();
  }

  @Test
  public void addEventInOriginalFilterIDsButQueueWasRemovedDueToSuccessfulRegistrationSoEventNotRedelivered() {
    // this test requires mock of EntryEventImpl instead of InternalCacheEvent
    when(cacheClientNotifier.getClientProxy(clientProxyMembershipId))
        .thenReturn(cacheClientProxy);
    when(cacheClientNotifier.getFilterClientIDs(internalCacheEvent, filterProfile, filterInfo,
        clientUpdateMessage))
            .thenReturn(asSet(clientProxyMembershipId));
    when(cacheClientProxy.getProxyID())
        .thenReturn(clientProxyMembershipId);
    when(internalCacheEvent.getRegion())
        .thenReturn(internalRegion);
    when(filterProfile.getFilterRoutingInfoPart2(null, internalCacheEvent))
        .thenReturn(filterRoutingInfo);
    when(filterRoutingInfo.getLocalFilterInfo())
        .thenReturn(filterInfo);
    when(internalRegion.getFilterProfile())
        .thenReturn(filterProfile);

    var readWriteLock = spy(new ReentrantReadWriteLock());
    var readLock = spy(readWriteLock.readLock());

    when(readWriteLock.readLock())
        .thenReturn(readLock);

    var clientRegistrationEventQueueManager =
        new ClientRegistrationEventQueueManager();

    var clientRegistrationEventQueue =
        clientRegistrationEventQueueManager.create(clientProxyMembershipId,
            new ConcurrentLinkedQueue<>(), readWriteLock);

    doAnswer((Answer<Void>) invocation -> {
      clientRegistrationEventQueueManager.drain(clientRegistrationEventQueue, cacheClientNotifier);
      invocation.callRealMethod();
      return null;
    })
        .when(readLock)
        .lock();

    clientRegistrationEventQueueManager.add(internalCacheEvent, clientUpdateMessage,
        clientUpdateMessage, asSet(clientProxyMembershipId), cacheClientNotifier);

    verify(cacheClientProxy, never()).deliverMessage(clientUpdateMessage);
  }

  @Test
  public void addEventWithClientTombstoneDoesNotExportNewValue() {
    var clientTombstoneMessage = mock(ClientTombstoneMessage.class);
    // this test requires mock of EntryEventImpl instead of InternalCacheEvent
    var entryEventImpl = mock(EntryEventImpl.class);

    var clientRegistrationEventQueueManager =
        new ClientRegistrationEventQueueManager();

    clientRegistrationEventQueueManager.add(entryEventImpl, clientTombstoneMessage,
        clientTombstoneMessage, asSet(mock(ClientProxyMembershipID.class)),
        mock(CacheClientNotifier.class));

    verify(entryEventImpl, never()).exportNewValue(clientTombstoneMessage);
  }

  private EntryEventImpl entryEventImpl() {
    var entryEventImpl = mock(EntryEventImpl.class);
    var operation = operation();

    when(entryEventImpl.getOperation())
        .thenReturn(operation);
    when(entryEventImpl.getRegion())
        .thenReturn(internalRegion);

    return entryEventImpl;
  }

  private Operation operation() {
    var operation = mock(Operation.class);

    when(operation.isEntry())
        .thenReturn(true);

    return operation;
  }
}
