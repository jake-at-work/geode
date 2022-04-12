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
package org.apache.geode.distributed;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ServerLauncher.Builder;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.process.ControllableProcess;

/**
 * Unit tests for {@link ServerLauncher}.
 *
 * @since GemFire 7.0
 */
public class ServerLauncherTest {

  @Before
  public void before() {
    DistributedSystem.removeSystem(InternalDistributedSystem.getConnectedInstance());
  }

  @Test
  public void constructorCorrectlySetsServerLauncherParameters() {
    var launcher = new Builder().setServerBindAddress(null).setServerPort(11235)
        .setMaxThreads(10).setMaxConnections(100).setMaxMessageCount(5).setMessageTimeToLive(10000)
        .setSocketBufferSize(2048).setHostNameForClients("hostName4Clients")
        .setDisableDefaultServer(Boolean.FALSE).build();

    assertThat(launcher).isNotNull();
    assertThat(ServerLauncherParameters.INSTANCE).isNotNull();
    assertThat(ServerLauncherParameters.INSTANCE.getPort()).isEqualTo(11235);
    assertThat(ServerLauncherParameters.INSTANCE.getMaxThreads()).isEqualTo(10);
    assertThat(ServerLauncherParameters.INSTANCE.getBindAddress()).isEqualTo(null);
    assertThat(ServerLauncherParameters.INSTANCE.getMaxConnections()).isEqualTo(100);
    assertThat(ServerLauncherParameters.INSTANCE.getMaxMessageCount()).isEqualTo(5);
    assertThat(ServerLauncherParameters.INSTANCE.getSocketBufferSize()).isEqualTo(2048);
    assertThat(ServerLauncherParameters.INSTANCE.getMessageTimeToLive()).isEqualTo(10000);
    assertThat(ServerLauncherParameters.INSTANCE.getHostnameForClients())
        .isEqualTo("hostName4Clients");
    assertThat(ServerLauncherParameters.INSTANCE.isDisableDefaultServer()).isFalse();
  }

  @Test
  public void isServingReturnsTrueWhenCacheHasOneCacheServer() {
    var cache = mock(Cache.class);
    var cacheServer = mock(CacheServer.class);
    when(cache.getCacheServers()).thenReturn(Collections.singletonList(cacheServer));

    var launcher = new Builder().build();

    assertThat(launcher.isServing(cache)).isTrue();
  }

  @Test
  public void isServingReturnsFalseWhenCacheHasZeroCacheServers() {
    var cache = mock(Cache.class);
    when(cache.getCacheServers()).thenReturn(Collections.emptyList());

    var launcher = new Builder().build();

    assertThat(launcher.isServing(cache)).isFalse();
  }

  @Test
  public void reconnectedCacheIsClosed() {
    var cache = mock(Cache.class, "Cache");
    var reconnectedCache = mock(Cache.class, "ReconnectedCache");
    when(cache.isReconnecting()).thenReturn(false).thenReturn(false).thenReturn(true);
    when(cache.getCacheServers()).thenReturn(Collections.emptyList());
    when(cache.getReconnectedCache()).thenReturn(reconnectedCache);

    new Builder().setCache(cache).build().waitOnServer();

    verify(cache, atLeast(3)).isReconnecting();
    verify(cache).getReconnectedCache();
    verify(reconnectedCache).close();
  }

  @Test
  public void isRunningReturnsTrueWhenRunningIsSetTrue() {
    var launcher = new Builder().build();

    launcher.running.set(true);

    assertThat(launcher.isRunning()).isTrue();
  }

  @Test
  public void isRunningReturnsFalseWhenRunningIsSetFalse() {
    var launcher = new Builder().build();

    launcher.running.set(false);

    assertThat(launcher.isRunning()).isFalse();
  }

  @Test
  public void reconnectingDistributedSystemIsDisconnectedOnStop() {
    var cache = mock(Cache.class, "Cache");
    var system = mock(DistributedSystem.class, "DistributedSystem");
    var reconnectedCache = mock(Cache.class, "ReconnectedCache");
    when(cache.isReconnecting()).thenReturn(true);
    when(cache.getReconnectedCache()).thenReturn(reconnectedCache);
    when(reconnectedCache.isReconnecting()).thenReturn(true);
    when(reconnectedCache.getReconnectedCache()).thenReturn(null);
    when(reconnectedCache.getDistributedSystem()).thenReturn(system);

    var launcher = new Builder().setCache(cache).build();
    launcher.running.set(true);
    launcher.stop();

    verify(cache).isReconnecting();
    verify(cache).getReconnectedCache();
    verify(cache).isReconnecting();
    verify(cache).getReconnectedCache();
    verify(reconnectedCache).getDistributedSystem();
    verify(system).stopReconnecting();
    verify(reconnectedCache).close();
  }

  @Test
  public void isWaitingReturnsTrueWhenSystemIsConnected() {
    var cache = mock(Cache.class, "Cache");
    var system = mock(DistributedSystem.class, "DistributedSystem");
    when(cache.getDistributedSystem()).thenReturn(system);
    when(system.isConnected()).thenReturn(true);

    var launcher = new Builder().build();
    launcher.running.set(true);

    assertThat(launcher.isWaiting(cache)).isTrue();
  }

  @Test
  public void isWaitingReturnsFalseWhenSystemIsNotConnected() {
    var cache = mock(Cache.class, "Cache");
    var system = mock(DistributedSystem.class, "DistributedSystem");
    when(cache.getDistributedSystem()).thenReturn(system);
    when(system.isConnected()).thenReturn(false);
    when(cache.isReconnecting()).thenReturn(false);

    var launcher = new Builder().setMemberName("serverOne").build();
    launcher.running.set(true);

    assertThat(launcher.isWaiting(cache)).isFalse();
  }

  @Test
  public void isWaitingReturnsFalseByDefault() {
    var launcher = new Builder().build();

    assertThat(launcher.isWaiting(null)).isFalse();
  }

  @Test
  public void isWaitingReturnsFalseWhenNotRunning() {
    var launcher = new Builder().build();

    launcher.running.set(false);

    assertThat(launcher.isWaiting(null)).isFalse();
  }

  @Test
  public void isDisableDefaultServerReturnsFalseByDefault() {
    var launcher = new Builder().build();

    assertThat(launcher.isDisableDefaultServer()).isFalse();
  }

  @Test
  public void isDefaultServerEnabledForCacheReturnsTrueByDefault() {
    var cache = mock(Cache.class, "Cache");

    var launcher = new Builder().build();

    assertThat(launcher.isDefaultServerEnabled(cache)).isTrue();
  }

  @Test
  public void isDefaultServerEnabledForNullThrowsNullPointerException() {
    var launcher = new Builder().build();

    assertThatThrownBy(() -> launcher.isDefaultServerEnabled(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void isDefaultServerEnabledReturnsFalseWhenCacheServersExist() {
    var cache = mock(Cache.class, "Cache");
    var cacheServer = mock(CacheServer.class, "CacheServer");
    when(cache.getCacheServers()).thenReturn(Collections.singletonList(cacheServer));

    var launcher = new Builder().build();

    assertThat(launcher.isDefaultServerEnabled(cache)).isFalse();
  }

  @Test
  public void isDisableDefaultServerReturnsTrueWhenDisabled() {
    var launcher = new Builder().setDisableDefaultServer(true).build();

    assertThat(launcher.isDisableDefaultServer()).isTrue();
  }

  @Test
  public void isDefaultServerEnabledReturnsFalseWhenDefaultServerDisabledIsTrueAndNoCacheServersExist() {
    var cache = mock(Cache.class, "Cache");
    when(cache.getCacheServers()).thenReturn(Collections.emptyList());

    var launcher = new Builder().setDisableDefaultServer(true).build();

    assertThat(launcher.isDefaultServerEnabled(cache)).isFalse();
  }

  @Test
  public void isDefaultServerEnabledReturnsFalseWhenDefaultServerDisabledIsTrueAndCacheServersExist() {
    var cache = mock(Cache.class, "Cache");
    var cacheServer = mock(CacheServer.class, "CacheServer");
    when(cache.getCacheServers()).thenReturn(Collections.singletonList(cacheServer));

    var launcher = new Builder().setDisableDefaultServer(true).build();

    assertThat(launcher.isDefaultServerEnabled(cache)).isFalse();
  }

  @Test
  public void startCacheServerStartsCacheServerWithBuilderValues() throws IOException {
    Cache cache = createCache();
    var cacheServer = mock(CacheServer.class, "CacheServer");
    when(cache.getCacheServers()).thenReturn(Collections.emptyList());
    when(cache.addCacheServer()).thenReturn(cacheServer);
    var launcher = new Builder()
        .setServerBindAddress(null)
        .setServerPort(11235)
        .setMaxThreads(10)
        .setMaxConnections(100)
        .setMaxMessageCount(5)
        .setMessageTimeToLive(10000)
        .setSocketBufferSize(2048)
        .setHostNameForClients("hostName4Clients")
        .setServerLauncherCacheProvider((a, b) -> cache)
        .setControllableProcessFactory(() -> mock(ControllableProcess.class))
        .build();

    launcher.start();

    verify(cacheServer).setBindAddress(null);
    verify(cacheServer).setPort(eq(11235));
    verify(cacheServer).setMaxThreads(10);
    verify(cacheServer).setMaxConnections(100);
    verify(cacheServer).setMaximumMessageCount(5);
    verify(cacheServer).setMessageTimeToLive(10000);
    verify(cacheServer).setSocketBufferSize(2048);
    verify(cacheServer).setHostnameForClients("hostName4Clients");
    verify(cacheServer).start();
  }

  @Test
  public void startCacheServerDoesNothingWhenDefaultServerDisabled() {
    Cache cache = createCache();
    var cacheServer = mock(CacheServer.class, "CacheServer");
    when(cache.getCacheServers()).thenReturn(Collections.emptyList());
    when(cache.addCacheServer()).thenReturn(cacheServer);
    var launcher = new Builder()
        .setDisableDefaultServer(true)
        .setServerLauncherCacheProvider((a, b) -> cache)
        .setControllableProcessFactory(() -> mock(ControllableProcess.class))
        .build();

    launcher.start();

    verifyNoMoreInteractions(cacheServer);
  }

  @Test
  public void startCacheServerDoesNothingWhenCacheServerAlreadyExists() {
    Cache cache = createCache();
    var cacheServer1 = mock(CacheServer.class, "CacheServer1");
    var cacheServer2 = mock(CacheServer.class, "CacheServer2");
    when(cache.getCacheServers()).thenReturn(Collections.singletonList(cacheServer1));
    when(cache.addCacheServer()).thenReturn(cacheServer1);
    var launcher = new Builder()
        .setControllableProcessFactory(() -> mock(ControllableProcess.class))
        .setDisableDefaultServer(true)
        .setServerLauncherCacheProvider((a, b) -> cache)
        .build();

    launcher.start();

    verifyNoMoreInteractions(cacheServer2);
  }

  @Test
  public void startRunsCompletionActionAfterStartupTasksComplete() {
    var startupCompletionAction = mock(Runnable.class);
    @SuppressWarnings("unchecked")
    Consumer<Throwable> startupExceptionAction = mock(Consumer.class);
    var internalResourceManager = mock(InternalResourceManager.class);
    Cache cache = createCache(internalResourceManager);
    var serverLauncher = new Builder()
        .setControllableProcessFactory(() -> mock(ControllableProcess.class))
        .setDisableDefaultServer(true)
        .setServerLauncherCacheProvider((a, b) -> cache)
        .setStartupCompletionAction(startupCompletionAction)
        .setStartupExceptionAction(startupExceptionAction)
        .build();

    when(internalResourceManager.allOfStartupTasks())
        .thenReturn(CompletableFuture.completedFuture(null));

    serverLauncher.start();

    verify(startupCompletionAction).run();
    verifyNoMoreInteractions(startupExceptionAction);
  }

  @Test
  public void startRunsExceptionActionAfterStartupTasksError() {
    var startupCompletionAction = mock(Runnable.class);
    @SuppressWarnings("unchecked")
    Consumer<Throwable> startupExceptionAction = mock(Consumer.class);
    var internalResourceManager = mock(InternalResourceManager.class);
    Cache cache = createCache(internalResourceManager);
    var serverLauncher = new Builder()
        .setControllableProcessFactory(() -> mock(ControllableProcess.class))
        .setDisableDefaultServer(true)
        .setServerLauncherCacheProvider((a, b) -> cache)
        .setStartupExceptionAction(startupExceptionAction)
        .build();

    when(internalResourceManager.allOfStartupTasks())
        .thenReturn(completedExceptionallyFuture());

    serverLauncher.start();

    verify(startupExceptionAction).accept(any());
    verifyNoMoreInteractions(startupCompletionAction);
  }

  @Test
  public void startUsesCacheProviderFromBuilder() {
    var serverLauncherCacheProvider =
        mock(ServerLauncherCacheProvider.class);
    Cache cacheFromBuilder = createCache();
    when(serverLauncherCacheProvider.createCache(any(), any())).thenReturn(cacheFromBuilder);

    var serverLauncher = new Builder()
        .setControllableProcessFactory(() -> mock(ControllableProcess.class))
        .setDisableDefaultServer(true)
        .setServerLauncherCacheProvider(serverLauncherCacheProvider)
        .build();
    serverLauncher.start();

    assertThat(serverLauncher.getCache()).isSameAs(cacheFromBuilder);
  }

  @Test
  public void startUsesControllableProcessFromBuilder() {
    var serverLauncherCacheProvider =
        mock(ServerLauncherCacheProvider.class);
    var controllableProcess = mock(ControllableProcess.class);
    Cache cacheFromBuilder = createCache();
    when(serverLauncherCacheProvider.createCache(any(), any())).thenReturn(cacheFromBuilder);

    var serverLauncher = new Builder()
        .setControllableProcessFactory(() -> controllableProcess)
        .setDisableDefaultServer(true)
        .setServerLauncherCacheProvider(serverLauncherCacheProvider)
        .build();
    serverLauncher.start();

    serverLauncher.stop();

    verify(controllableProcess).stop(anyBoolean());
  }

  @Test
  public void startWaitsForStartupTasksToComplete() {
    var cacheProvider = mock(ServerLauncherCacheProvider.class);
    var internalResourceManager = mock(InternalResourceManager.class);
    var cacheFromBuilder = createCache(internalResourceManager);
    CompletableFuture<Void> startupTasks = spy(new CompletableFuture<>());
    when(cacheProvider.createCache(any(), any()))
        .thenReturn(cacheFromBuilder);
    when(internalResourceManager.allOfStartupTasks())
        .thenReturn(startupTasks);

    var serverLauncher = new Builder()
        .setControllableProcessFactory(() -> mock(ControllableProcess.class))
        .setDisableDefaultServer(true)
        .setServerLauncherCacheProvider(cacheProvider)
        .build();

    var serverLauncherStart = CompletableFuture.runAsync(serverLauncher::start);

    await().untilAsserted(() -> verify(startupTasks).thenRun(any()));
    assertThat(serverLauncherStart).isNotDone();

    startupTasks.complete(null);

    await().untilAsserted(() -> assertThat(serverLauncherStart).isDone());
  }

  private CompletableFuture<Void> completedExceptionallyFuture() {
    var completedExceptionallyFuture = new CompletableFuture<Void>();
    completedExceptionallyFuture.completeExceptionally(new RuntimeException());
    return completedExceptionallyFuture;
  }

  private InternalCache createCache() {
    return createCache(mock(InternalResourceManager.class));
  }

  private InternalCache createCache(InternalResourceManager internalResourceManager) {
    var cache = mock(InternalCache.class);
    var cacheServer = mock(CacheServer.class);

    when(cache.addCacheServer()).thenReturn(cacheServer);
    when(cache.getResourceManager()).thenReturn(internalResourceManager);
    when(internalResourceManager.allOfStartupTasks())
        .thenReturn(CompletableFuture.completedFuture(null));

    return cache;
  }
}
