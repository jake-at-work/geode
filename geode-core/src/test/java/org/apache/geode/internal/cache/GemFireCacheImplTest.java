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

import static org.apache.geode.distributed.internal.ClusterDistributionManager.LOCATOR_DM_TYPE;
import static org.apache.geode.distributed.internal.InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS_PROPERTY;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.NotSerializableException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import io.micrometer.core.instrument.MeterRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import org.apache.geode.CancelCriterion;
import org.apache.geode.SerializationException;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.cache.GemFireCacheImpl.ReplyProcessor21Factory;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.ResourceAdvisor;
import org.apache.geode.internal.cache.eviction.HeapEvictor;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.internal.JmxManagerAdvisor;
import org.apache.geode.pdx.internal.TypeRegistry;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

/**
 * Unit tests for {@link GemFireCacheImpl}.
 */
public class GemFireCacheImplTest {

  private CacheConfig cacheConfig;
  private InternalDistributedSystem internalDistributedSystem;
  private PoolFactory poolFactory;
  private ReplyProcessor21Factory replyProcessor21Factory;
  private TypeRegistry typeRegistry;
  private final InternalResourceManager internalResourceManager =
      mock(InternalResourceManager.class);

  private GemFireCacheImpl gemFireCacheImpl;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @Before
  public void setUp() {
    cacheConfig = mock(CacheConfig.class);
    internalDistributedSystem = mock(InternalDistributedSystem.class);
    poolFactory = mock(PoolFactory.class);
    replyProcessor21Factory = mock(ReplyProcessor21Factory.class);
    typeRegistry = mock(TypeRegistry.class);

    var distributionConfig = mock(DistributionConfig.class);
    when(distributionConfig.getUseSharedConfiguration()).thenReturn(false);
    var distributionManager = mock(DistributionManager.class);
    var replyProcessor21 = mock(ReplyProcessor21.class);

    when(distributionConfig.getSecurityProps())
        .thenReturn(new Properties());
    when(internalDistributedSystem.getConfig())
        .thenReturn(distributionConfig);
    when(internalDistributedSystem.getDistributionManager())
        .thenReturn(distributionManager);
    when(internalDistributedSystem.getCancelCriterion())
        .thenReturn(mock(CancelCriterion.class));
    when(replyProcessor21.getProcessorId())
        .thenReturn(21);
    when(replyProcessor21Factory.create(any(), any()))
        .thenReturn(replyProcessor21);

    gemFireCacheImpl = gemFireCacheImpl(false);
  }

  @After
  public void tearDown() {
    if (gemFireCacheImpl != null) {
      gemFireCacheImpl.close();
    }
  }

  @Test
  public void canBeMocked() {
    var gemFireCacheImpl = mock(GemFireCacheImpl.class);
    var internalResourceManager = mock(InternalResourceManager.class);

    when(gemFireCacheImpl.getInternalResourceManager())
        .thenReturn(internalResourceManager);

    assertThat(gemFireCacheImpl.getInternalResourceManager())
        .isSameAs(internalResourceManager);
  }

  @Test
  public void checkPurgeCCPTimer() {
    var cacheClientProxyTimer = mock(SystemTimer.class);
    gemFireCacheImpl.setCCPTimer(cacheClientProxyTimer);

    for (var i = 1; i < GemFireCacheImpl.PURGE_INTERVAL; i++) {
      gemFireCacheImpl.purgeCCPTimer();
      verify(cacheClientProxyTimer,
          times(0))
              .timerPurge();
    }

    gemFireCacheImpl.purgeCCPTimer();
    verify(cacheClientProxyTimer,
        times(1))
            .timerPurge();

    for (var i = 1; i < GemFireCacheImpl.PURGE_INTERVAL; i++) {
      gemFireCacheImpl.purgeCCPTimer();
      verify(cacheClientProxyTimer,
          times(1))
              .timerPurge();
    }

    gemFireCacheImpl.purgeCCPTimer();
    verify(cacheClientProxyTimer,
        times(2))
            .timerPurge();
  }

  @Test
  public void registerPdxMetaDataThrowsIfInstanceNotSerializable() {
    var thrown = catchThrowable(() -> gemFireCacheImpl.registerPdxMetaData(new Object()));

    assertThat(thrown)
        .isInstanceOf(SerializationException.class)
        .hasMessage("Serialization failed")
        .hasCauseInstanceOf(NotSerializableException.class);
  }

  @Test
  public void registerPdxMetaDataThrowsIfInstanceIsNotPDX() {
    var thrown = catchThrowable(() -> gemFireCacheImpl.registerPdxMetaData("string"));

    assertThat(thrown)
        .isInstanceOf(SerializationException.class)
        .hasMessage("The instance is not PDX serializable");
  }

  @Test
  public void checkThatAsyncEventListenersUseAllThreadsInPool() {
    gemFireCacheImpl = gemFireCacheImpl(true);
    var eventThreadPoolExecutor =
        (ThreadPoolExecutor) gemFireCacheImpl.getEventThreadPool();

    assertThat(eventThreadPoolExecutor.getCompletedTaskCount())
        .isZero();
    assertThat(eventThreadPoolExecutor.getActiveCount())
        .isZero();

    var eventThreadLimit = GemFireCacheImpl.EVENT_THREAD_LIMIT;
    var threadLatch = new CountDownLatch(eventThreadLimit);

    for (var i = 1; i <= eventThreadLimit; i++) {
      eventThreadPoolExecutor.execute(() -> {
        threadLatch.countDown();
        try {
          threadLatch.await();
        } catch (InterruptedException ignore) {
          // ignored
        }
      });
    }

    await().untilAsserted(() -> assertThat(eventThreadPoolExecutor.getCompletedTaskCount())
        .isEqualTo(eventThreadLimit));
  }

  @Test
  public void getCacheClosedException_withoutReasonOrCauseGivesExceptionWithoutEither() {
    var value = gemFireCacheImpl.getCacheClosedException(null, null);

    assertThat(value.getCause())
        .isNull();
    assertThat(value.getMessage())
        .isNull();
  }

  @Test
  public void getCacheClosedException_withoutCauseGivesExceptionWithReason() {
    var value = gemFireCacheImpl.getCacheClosedException("message");

    assertThat(value.getCause())
        .isNull();
    assertThat(value.getMessage())
        .isEqualTo("message");
  }

  @Test
  public void getCacheClosedException_returnsExceptionWithProvidedCauseAndReason() {
    var cause = new Throwable();

    var value = gemFireCacheImpl.getCacheClosedException("message", cause);

    assertThat(value.getCause())
        .isEqualTo(cause);
    assertThat(value.getMessage())
        .isEqualTo("message");
  }

  @Test
  public void getCacheClosedException_prefersGivenCauseWhenDisconnectExceptionExists() {
    gemFireCacheImpl.setDisconnectCause(new Throwable("disconnectCause"));
    var cause = new Throwable();

    var value = gemFireCacheImpl.getCacheClosedException("message", cause);

    assertThat(value.getCause())
        .isEqualTo(cause);
    assertThat(value.getMessage())
        .isEqualTo("message");
  }

  @Test
  public void getCacheClosedException_withoutCauseGiven_providesDisconnectExceptionIfExists() {
    var disconnectCause = new Throwable("disconnectCause");
    gemFireCacheImpl.setDisconnectCause(disconnectCause);

    var value = gemFireCacheImpl.getCacheClosedException("message");

    assertThat(value.getCause())
        .isEqualTo(disconnectCause);
    assertThat(value.getMessage())
        .isEqualTo("message");
  }

  @Test
  public void getCacheClosedException_returnsExceptionWithProvidedReason() {
    var value = gemFireCacheImpl.getCacheClosedException("message");

    assertThat(value.getMessage())
        .isEqualTo("message");
    assertThat(value.getCause())
        .isNull();
  }

  @Test
  public void getCacheClosedException_returnsExceptionWithoutMessageWhenReasonNotGiven() {
    var value = gemFireCacheImpl.getCacheClosedException(null);

    assertThat(value.getMessage())
        .isEqualTo(null);
    assertThat(value.getCause())
        .isNull();
  }

  @Test
  public void getCacheClosedException_returnsExceptionWithDisconnectCause() {
    var disconnectCause = new Throwable("disconnectCause");
    gemFireCacheImpl.setDisconnectCause(disconnectCause);

    var value = gemFireCacheImpl.getCacheClosedException("message");

    assertThat(value.getMessage())
        .isEqualTo("message");
    assertThat(value.getCause())
        .isEqualTo(disconnectCause);
  }

  @Test
  public void addGatewayReceiverDoesNotAllowMoreThanOneGatewayReceiver() {
    var receiver = mock(GatewayReceiver.class);
    var receiver2 = mock(GatewayReceiver.class);
    gemFireCacheImpl.addGatewayReceiver(receiver);

    gemFireCacheImpl.addGatewayReceiver(receiver2);

    assertThat(gemFireCacheImpl.getGatewayReceivers())
        .containsOnly(receiver2);
  }

  @Test
  public void addGatewayReceiverRequiresSuppliedGatewayReceiver() {
    var thrown = catchThrowable(() -> gemFireCacheImpl.addGatewayReceiver(null));

    assertThat(thrown)
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void addGatewayReceiverAddsGatewayReceiver() {
    var receiver = mock(GatewayReceiver.class);

    gemFireCacheImpl.addGatewayReceiver(receiver);

    assertThat(gemFireCacheImpl.getGatewayReceivers())
        .hasSize(1);
  }

  @Test
  public void removeGatewayReceiverRemovesGatewayReceiver() {
    var receiver = mock(GatewayReceiver.class);
    gemFireCacheImpl.addGatewayReceiver(receiver);

    gemFireCacheImpl.removeGatewayReceiver(receiver);

    assertThat(gemFireCacheImpl.getGatewayReceivers())
        .isEmpty();
  }

  @Test
  public void addCacheServerAddsOneCacheServer() {
    gemFireCacheImpl.addCacheServer();

    assertThat(gemFireCacheImpl.getCacheServers())
        .hasSize(1);
  }

  @Test
  public void removeCacheServerRemovesSpecifiedCacheServer() {
    var cacheServer = gemFireCacheImpl.addCacheServer();

    gemFireCacheImpl.removeCacheServer(cacheServer);

    assertThat(gemFireCacheImpl.getCacheServers())
        .isEmpty();
  }

  @Test
  public void testIsMisConfigured() {
    var clusterProps = new Properties();
    var serverProps = new Properties();

    // both does not have the key
    assertThat(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key")).isFalse();

    // cluster has the key, not the server
    clusterProps.setProperty("key", "value");
    assertThat(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key")).isFalse();

    clusterProps.setProperty("key", "");
    assertThat(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key")).isFalse();

    // server has the key, not the cluster
    clusterProps.clear();
    serverProps.clear();
    serverProps.setProperty("key", "value");
    assertThat(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key")).isTrue();

    serverProps.setProperty("key", "");
    assertThat(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key")).isFalse();

    // server has the key, not the cluster
    clusterProps.clear();
    serverProps.clear();
    clusterProps.setProperty("key", "");
    serverProps.setProperty("key", "value");
    assertThat(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key")).isTrue();

    serverProps.setProperty("key", "");
    assertThat(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key")).isFalse();

    // server and cluster has the same value
    clusterProps.clear();
    serverProps.clear();
    clusterProps.setProperty("key", "value");
    serverProps.setProperty("key", "value");
    assertThat(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key")).isFalse();

    clusterProps.setProperty("key", "");
    serverProps.setProperty("key", "");
    assertThat(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key")).isFalse();

    // server and cluster has the different value
    clusterProps.clear();
    serverProps.clear();
    clusterProps.setProperty("key", "value1");
    serverProps.setProperty("key", "value2");
    assertThat(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key")).isTrue();

    clusterProps.setProperty("key", "value1");
    serverProps.setProperty("key", "");
    assertThat(GemFireCacheImpl.isMisConfigured(clusterProps, serverProps, "key")).isFalse();
  }

  @Test
  public void clientCacheDoesNotRequestClusterConfig() {
    System.setProperty(ALLOW_MULTIPLE_SYSTEMS_PROPERTY, "true");
    gemFireCacheImpl = mock(GemFireCacheImpl.class);
    when(internalDistributedSystem.getCache()).thenReturn(gemFireCacheImpl);

    new InternalCacheBuilder()
        .setIsClient(true)
        .create(internalDistributedSystem);

    verify(gemFireCacheImpl,
        times(0))
            .requestSharedConfiguration();
    verify(gemFireCacheImpl,
        times(0))
            .applyJarAndXmlFromClusterConfig();
  }

  @Test
  public void getMeterRegistry_returnsTheSystemMeterRegistry() {
    var systemMeterRegistry = mock(MeterRegistry.class);
    when(internalDistributedSystem.getMeterRegistry()).thenReturn(systemMeterRegistry);

    assertThat(gemFireCacheImpl.getMeterRegistry())
        .isSameAs(systemMeterRegistry);
  }

  @Test
  public void addGatewayReceiverServer_requiresPreviouslyAddedGatewayReceiver() {
    var thrown = catchThrowable(
        () -> gemFireCacheImpl.addGatewayReceiverServer(mock(GatewayReceiver.class)));

    assertThat(thrown)
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void addGatewayReceiverServer_requiresSuppliedGatewayReceiver() {
    var gatewayReceiver = mock(GatewayReceiver.class);
    gemFireCacheImpl.addGatewayReceiver(gatewayReceiver);

    var thrown = catchThrowable(() -> gemFireCacheImpl.addGatewayReceiverServer(null));

    assertThat(thrown)
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void addGatewayReceiverServer_addsCacheServer() {
    var gatewayReceiver = mock(GatewayReceiver.class);
    gemFireCacheImpl.addGatewayReceiver(gatewayReceiver);

    var receiverServer = gemFireCacheImpl.addGatewayReceiverServer(gatewayReceiver);

    assertThat(gemFireCacheImpl.getCacheServersAndGatewayReceiver())
        .containsOnly(receiverServer);
  }

  @Test
  public void getCacheServers_isEmptyByDefault() {
    var value = gemFireCacheImpl.getCacheServers();

    assertThat(value)
        .isEmpty();
  }

  @Test
  public void getCacheServers_returnsAddedCacheServer() {
    var cacheServer = gemFireCacheImpl.addCacheServer();

    var value = gemFireCacheImpl.getCacheServers();

    assertThat(value)
        .containsExactly(cacheServer);
  }

  @Test
  public void getCacheServers_returnsMultipleAddedCacheServers() {
    var cacheServer1 = gemFireCacheImpl.addCacheServer();
    var cacheServer2 = gemFireCacheImpl.addCacheServer();
    var cacheServer3 = gemFireCacheImpl.addCacheServer();

    var value = gemFireCacheImpl.getCacheServers();

    assertThat(value)
        .containsExactlyInAnyOrder(cacheServer1, cacheServer2, cacheServer3);
  }

  @Test
  public void getCacheServers_isStillEmptyAfterAddingGatewayReceiverServer() {
    var gatewayReceiver = mock(GatewayReceiver.class);
    gemFireCacheImpl.addGatewayReceiver(gatewayReceiver);
    gemFireCacheImpl.addGatewayReceiverServer(gatewayReceiver);

    var value = gemFireCacheImpl.getCacheServers();

    assertThat(value)
        .isEmpty();
  }

  @Test
  public void getCacheServers_doesNotIncludeGatewayReceiverServer() {
    var cacheServer1 = gemFireCacheImpl.addCacheServer();
    var cacheServer2 = gemFireCacheImpl.addCacheServer();
    var cacheServer3 = gemFireCacheImpl.addCacheServer();
    var gatewayReceiver = mock(GatewayReceiver.class);
    gemFireCacheImpl.addGatewayReceiver(gatewayReceiver);
    gemFireCacheImpl.addGatewayReceiverServer(gatewayReceiver);

    var value = gemFireCacheImpl.getCacheServers();

    assertThat(value)
        .containsExactlyInAnyOrder(cacheServer1, cacheServer2, cacheServer3);
  }

  @Test
  public void getCacheServersAndGatewayReceiver_isEmptyByDefault() {
    var value = gemFireCacheImpl.getCacheServersAndGatewayReceiver();

    assertThat(value)
        .isEmpty();
  }

  @Test
  public void getCacheServersAndGatewayReceiver_includesCacheServers() {
    var cacheServer1 = (InternalCacheServer) gemFireCacheImpl.addCacheServer();
    var cacheServer2 = (InternalCacheServer) gemFireCacheImpl.addCacheServer();
    var cacheServer3 = (InternalCacheServer) gemFireCacheImpl.addCacheServer();

    var value = gemFireCacheImpl.getCacheServersAndGatewayReceiver();

    assertThat(value)
        .containsExactlyInAnyOrder(cacheServer1, cacheServer2, cacheServer3);
  }

  @Test
  public void getCacheServersAndGatewayReceiver_includesGatewayReceiver() {
    var gatewayReceiver = mock(GatewayReceiver.class);
    gemFireCacheImpl.addGatewayReceiver(gatewayReceiver);
    var receiverServer = gemFireCacheImpl.addGatewayReceiverServer(gatewayReceiver);

    var value = gemFireCacheImpl.getCacheServersAndGatewayReceiver();

    assertThat(value)
        .containsExactly(receiverServer);
  }

  @Test
  public void getCacheServersAndGatewayReceiver_includesCacheServersAndGatewayReceiver() {
    var cacheServer1 = (InternalCacheServer) gemFireCacheImpl.addCacheServer();
    var cacheServer2 = (InternalCacheServer) gemFireCacheImpl.addCacheServer();
    var cacheServer3 = (InternalCacheServer) gemFireCacheImpl.addCacheServer();
    var gatewayReceiver = mock(GatewayReceiver.class);
    gemFireCacheImpl.addGatewayReceiver(gatewayReceiver);
    var receiverServer = gemFireCacheImpl.addGatewayReceiverServer(gatewayReceiver);

    var value = gemFireCacheImpl.getCacheServersAndGatewayReceiver();

    assertThat(value)
        .containsExactlyInAnyOrder(cacheServer1, cacheServer2, cacheServer3, receiverServer);
  }

  @Test
  public void isServer_isFalseByDefault() {
    var value = gemFireCacheImpl.isServer();

    assertThat(value)
        .isFalse();
  }

  @Test
  public void isServer_isTrueIfIsServerIsSet() {
    gemFireCacheImpl.setIsServer(true);

    var value = gemFireCacheImpl.isServer();

    assertThat(value)
        .isTrue();
  }

  @Test
  public void isServer_isTrueIfCacheServerExists() {
    gemFireCacheImpl.addCacheServer();

    var value = gemFireCacheImpl.isServer();

    assertThat(value)
        .isTrue();
  }

  @Test
  public void isServer_isFalseEvenIfGatewayReceiverServerExists() {
    var gatewayReceiver = mock(GatewayReceiver.class);
    gemFireCacheImpl.addGatewayReceiver(gatewayReceiver);
    gemFireCacheImpl.addGatewayReceiverServer(gatewayReceiver);

    var value = gemFireCacheImpl.isServer();

    assertThat(value)
        .isFalse();
  }

  @Test
  public void getCacheServers_isCanonical() {
    assertThat(gemFireCacheImpl.getCacheServers())
        .isSameAs(gemFireCacheImpl.getCacheServers());
  }

  @Test
  public void testMultiThreadLockUnlockDiskStore() throws InterruptedException {
    var nThread = 10;
    var diskStoreName = "MyDiskStore";
    var nTrue = new AtomicInteger();
    var nFalse = new AtomicInteger();
    IntStream.range(0, nThread).forEach(tid -> executorServiceRule.submit(() -> {
      try {
        var lockResult = gemFireCacheImpl.doLockDiskStore(diskStoreName);
        if (lockResult) {
          nTrue.incrementAndGet();
        } else {
          nFalse.incrementAndGet();
        }
      } finally {
        var unlockResult = gemFireCacheImpl.doUnlockDiskStore(diskStoreName);
        if (unlockResult) {
          nTrue.incrementAndGet();
        } else {
          nFalse.incrementAndGet();
        }
      }
    }));
    executorServiceRule.getExecutorService().shutdown();
    var terminated =
        executorServiceRule.getExecutorService()
            .awaitTermination(GeodeAwaitility.getTimeout().toNanos(), TimeUnit.NANOSECONDS);
    assertThat(terminated).isTrue();
    // 1 thread returns true for locking, all 10 threads return true for unlocking
    assertThat(nTrue.get()).isEqualTo(11);
    // 9 threads return false for locking
    assertThat(nFalse.get()).isEqualTo(9);
  }

  @Test
  public void cacheXmlGenerationErrorDisablesAutoReconnect() {
    gemFireCacheImpl.prepareForReconnect((printWriter) -> {
      throw new RuntimeException("error generating cache XML");
    });
    verify(internalDistributedSystem.getConfig()).setDisableAutoReconnect(Boolean.TRUE);
    verify(cacheConfig, never()).setCacheXMLDescription(isA(String.class));
  }

  @Test
  public void anythingThrownDuringInitializeDeclarativeCacheShouldBeCaughtAndFinallyCloseCache() {
    var internalDistributedMember = mock(InternalDistributedMember.class);
    when(internalDistributedSystem.getDistributedMember()).thenReturn(internalDistributedMember);
    var distributionConfig = mock(DistributionConfig.class);
    when(internalDistributedSystem.getConfig()).thenReturn(distributionConfig);
    var file = mock(File.class);
    when(distributionConfig.getDeployWorkingDir()).thenReturn(file);
    when(file.canWrite()).thenReturn(true);
    when(file.listFiles()).thenReturn(new File[0]);
    when(file.list()).thenReturn(new String[0]);
    when(internalDistributedMember.getVmKind()).thenReturn(LOCATOR_DM_TYPE);
    when(internalDistributedSystem.getDistributedMember())
        .thenThrow(new Error("Expected error by test."));
    assertThat(gemFireCacheImpl.isClosed()).isFalse();
    assertThatThrownBy(() -> gemFireCacheImpl.initialize()).isInstanceOf(Error.class)
        .hasMessageContaining("Expected error by test.");
    assertThat(gemFireCacheImpl.isClosed()).isTrue();
  }

  @Test
  public void getQueryMonitorReturnsNullGivenCriticalHeapPercentageOfZero() {
    when(internalResourceManager.getCriticalHeapPercentage()).thenReturn(0.0f);

    var queryMonitor = gemFireCacheImpl.getQueryMonitor();

    assertThat(queryMonitor).isNull();
  }

  @Test
  public void getQueryMonitorReturnsInstanceGivenCriticalHeapPercentage() {
    when(internalResourceManager.getCriticalHeapPercentage()).thenReturn(1.0f);

    var queryMonitor = gemFireCacheImpl.getQueryMonitor();

    assertThat(queryMonitor).isNotNull();
  }

  @Test
  public void getQueryMonitorDoesNotCareIfCriticalOffheapPercentageIsGreaterThanZero() {
    when(internalResourceManager.getCriticalHeapPercentage()).thenReturn(0.0f);
    when(internalResourceManager.getCriticalOffHeapPercentage()).thenReturn(1.0f);

    var queryMonitor = gemFireCacheImpl.getQueryMonitor();

    assertThat(queryMonitor).isNull();
  }

  @Test
  public void getQueryMonitorReturnsInstanceGivenMaxQueryExecutionTimeGreaterThanZero() {
    when(internalResourceManager.getCriticalHeapPercentage()).thenReturn(0.0f);
    final var originalValue = GemFireCacheImpl.MAX_QUERY_EXECUTION_TIME;
    GemFireCacheImpl.MAX_QUERY_EXECUTION_TIME = 1;
    try {
      var queryMonitor = gemFireCacheImpl.getQueryMonitor();
      assertThat(queryMonitor).isNotNull();
    } finally {
      GemFireCacheImpl.MAX_QUERY_EXECUTION_TIME = originalValue;
    }
  }

  @SuppressWarnings({"LambdaParameterHidesMemberVariable", "OverlyCoupledMethod", "unchecked"})
  private GemFireCacheImpl gemFireCacheImpl(boolean useAsyncEventListeners) {
    return new GemFireCacheImpl(
        false,
        poolFactory,
        internalDistributedSystem,
        cacheConfig,
        useAsyncEventListeners,
        typeRegistry,
        mock(Consumer.class),
        (properties, cacheConfigArg) -> mock(SecurityService.class),
        () -> true,
        mock(Function.class),
        mock(Function.class),
        (factory, clock) -> mock(CachePerfStats.class),
        mock(GemFireCacheImpl.TXManagerImplFactory.class),
        mock(Supplier.class),
        distributionAdvisee -> mock(ResourceAdvisor.class),
        mock(Function.class),
        jmxManagerAdvisee -> mock(JmxManagerAdvisor.class),
        internalCache -> internalResourceManager,
        () -> 1,
        (cache, statisticsClock) -> mock(HeapEvictor.class),
        mock(Runnable.class),
        mock(Runnable.class),
        mock(Runnable.class),
        mock(Function.class),
        mock(Consumer.class),
        mock(GemFireCacheImpl.TypeRegistryFactory.class),
        mock(Consumer.class),
        mock(Consumer.class),
        o -> mock(SystemTimer.class),
        internalCache -> mock(TombstoneService.class),
        internalDistributedSystem -> mock(ExpirationScheduler.class),
        file -> mock(DiskStoreMonitor.class),
        () -> mock(RegionEntrySynchronizationListener.class),
        mock(Function.class),
        mock(Function.class),
        mock(TXEntryStateFactory.class),
        replyProcessor21Factory);
  }
}
