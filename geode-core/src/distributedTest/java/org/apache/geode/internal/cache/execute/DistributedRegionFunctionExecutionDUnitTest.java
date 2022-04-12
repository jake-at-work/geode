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
package org.apache.geode.internal.cache.execute;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTHENTICATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.apache.geode.util.internal.GeodeGlossary.GEMFIRE_PREFIX;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.InternalClientCache;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.util.TypedFunctionService;
import org.apache.geode.security.templates.DummyAuthenticator;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.FunctionServiceTest;

@Category(FunctionServiceTest.class)
@SuppressWarnings("serial")
public class DistributedRegionFunctionExecutionDUnitTest implements Serializable {

  private static final AtomicReference<Region<?, ?>> REGION = new AtomicReference<>();
  private static final AtomicReference<CountDownLatch> LATCH = new AtomicReference<>();

  private final Set<String> filter = new HashSet<>();

  private String regionName;
  private String poolName;

  private VM empty;
  private VM normal;
  private VM replicate1;
  private VM replicate2;
  private VM replicate3;

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    empty = getController();
    replicate1 = getVM(0);
    replicate2 = getVM(1);
    replicate3 = getVM(2);
    normal = getVM(3);

    for (var vm : toArray(empty, normal, replicate1, replicate2, replicate3)) {
      // zero is no-op unless test replaces the reference
      vm.invoke(() -> LATCH.set(new CountDownLatch(0)));
    }

    regionName = getClass().getSimpleName() + "_region";
    poolName = getClass().getSimpleName() + "_pool";

    for (var i = 100; i < 120; i++) {
      filter.add("execKey-" + i);
    }
  }

  @After
  public void tearDown() {
    for (var vm : toArray(empty, normal, replicate1, replicate2, replicate3)) {
      vm.invoke(() -> LATCH.get().countDown());
    }
  }

  @Test
  public void testDistributedRegionFunctionExecutionOnDataPolicyEmpty() {
    for (var vm : toArray(empty, normal, replicate1, replicate2, replicate3)) {
      vm.invoke(() -> createCache(getDistributedSystemProperties()));
    }

    empty.invoke(() -> createRegion(DataPolicy.EMPTY));
    normal.invoke(() -> createRegion(DataPolicy.NORMAL));
    for (var vm : toArray(replicate1, replicate2, replicate3)) {
      vm.invoke(() -> createRegion(DataPolicy.REPLICATE));
    }

    empty.invoke(() -> {
      populateRegion(200);

      executeDistributedRegionFunction();
    });
  }

  @Test
  public void testDistributedRegionFunctionExecutionOnDataPolicyEmpty_SendException() {
    for (var vm : toArray(empty, normal, replicate1, replicate2, replicate3)) {
      vm.invoke(() -> createCache(getDistributedSystemProperties()));
    }

    empty.invoke(() -> createRegion(DataPolicy.EMPTY));
    normal.invoke(() -> createRegion(DataPolicy.NORMAL));
    for (var vm : toArray(replicate1, replicate2, replicate3)) {
      vm.invoke(() -> createRegion(DataPolicy.REPLICATE));
    }

    empty.invoke(() -> {
      populateRegion(200);

      executeResultWithExceptionFunction();
    });
  }

  @Test
  public void testDistributedRegionFunctionExecutionOnDataPolicyEmpty_NoLastResult() {
    for (var vm : toArray(empty, normal, replicate1, replicate2, replicate3)) {
      vm.invoke(() -> createCache(getDistributedSystemProperties()));
    }

    empty.invoke(() -> createRegion(DataPolicy.EMPTY));
    normal.invoke(() -> createRegion(DataPolicy.NORMAL));
    for (var vm : toArray(replicate1, replicate2, replicate3)) {
      vm.invoke(() -> createRegion(DataPolicy.REPLICATE));
    }

    empty.invoke(() -> {
      populateRegion(200);

      executeNoLastResultFunction();
    });
  }

  @Test
  public void testDistributedRegionFunctionExecutionOnDataPolicyNormal() {
    for (var vm : toArray(empty, normal, replicate1, replicate2, replicate3)) {
      vm.invoke(() -> createCache(getDistributedSystemProperties()));
    }

    empty.invoke(() -> createRegion(DataPolicy.EMPTY));
    normal.invoke(() -> createRegion(DataPolicy.NORMAL));
    for (var vm : toArray(replicate1, replicate2, replicate3)) {
      vm.invoke(() -> createRegion(DataPolicy.REPLICATE));
    }

    empty.invoke(() -> populateRegion(200));

    normal.invoke(() -> {
      var thrown = catchThrowable(this::executeDistributedRegionFunction);

      assertThat(thrown)
          .isInstanceOf(FunctionException.class)
          .hasMessage("Function execution on region with DataPolicy.NORMAL is not supported");
    });
  }

  @Test
  public void testDistributedRegionFunctionExecutionOnDataPolicyReplicate() {
    for (var vm : toArray(empty, normal, replicate1, replicate2, replicate3)) {
      vm.invoke(() -> createCache(getDistributedSystemProperties()));
    }

    empty.invoke(() -> createRegion(DataPolicy.EMPTY));
    normal.invoke(() -> createRegion(DataPolicy.NORMAL));
    for (var vm : toArray(replicate1, replicate2, replicate3)) {
      vm.invoke(() -> createRegion(DataPolicy.REPLICATE));
    }

    empty.invoke(() -> populateRegion(200));

    replicate1.invoke(this::executeDistributedRegionFunction);
  }

  @Test
  public void testDistributedRegionFunctionExecutionOnDataPolicyReplicateNotTimedOut()
      throws Exception {
    for (var vm : toArray(empty, normal, replicate1, replicate2, replicate3)) {
      vm.invoke(() -> {
        createCache(getDistributedSystemProperties());

        LATCH.set(new CountDownLatch(1));

        FunctionService.registerFunction(new DistributedRegionFunction());
        FunctionService.registerFunction(new LongRunningFunction(LATCH.get()));
      });
    }

    empty.invoke(() -> createRegion(DataPolicy.EMPTY));
    normal.invoke(() -> createRegion(DataPolicy.NORMAL));
    for (var vm : toArray(replicate1, replicate2, replicate3)) {
      vm.invoke(() -> createRegion(DataPolicy.REPLICATE));
    }

    empty.invoke(() -> populateRegion(200));

    AsyncInvocation executeFunctionInReplicate1 = replicate1.invokeAsync(() -> {
      var resultCollector = TypedFunctionService
          .<Void, String, List<String>>onRegion(getRegion())
          .withFilter(filter)
          .execute(LongRunningFunction.class.getSimpleName(), getTimeout().toMillis(),
              MILLISECONDS);

      assertThat(resultCollector.getResult().get(0))
          .isEqualTo("LongRunningFunction completed");
    });

    // how long LongRunningFunction runs for is now controlled here:
    Thread.sleep(2000);

    for (var vm : toArray(empty, normal, replicate1, replicate2, replicate3)) {
      vm.invoke(() -> {
        LATCH.get().countDown();
      });
    }

    executeFunctionInReplicate1.await();
  }

  @Test
  public void testDistributedRegionFunctionExecutionOnDataPolicyReplicateTimedOut() {
    for (var vm : toArray(empty, normal, replicate1, replicate2, replicate3)) {
      vm.invoke(() -> {
        createCache(getDistributedSystemProperties());

        LATCH.set(new CountDownLatch(1));

        FunctionService.registerFunction(new DistributedRegionFunction());
        FunctionService.registerFunction(new LongRunningFunction(LATCH.get()));
      });
    }

    empty.invoke(() -> createRegion(DataPolicy.EMPTY));
    normal.invoke(() -> createRegion(DataPolicy.NORMAL));
    for (var vm : toArray(replicate1, replicate2, replicate3)) {
      vm.invoke(() -> createRegion(DataPolicy.REPLICATE));
    }

    empty.invoke(() -> populateRegion(200));

    replicate1.invoke(() -> {
      var thrown = catchThrowable(() -> {
        TypedFunctionService
            .<Void, String, List<String>>onRegion(getRegion())
            .withFilter(filter)
            .execute(LongRunningFunction.class.getSimpleName(), 1000, MILLISECONDS);
      });

      assertThat(thrown)
          .hasCauseInstanceOf(FunctionException.class)
          .hasMessageContaining("All results not received in time provided");
    });
  }

  @Test
  public void testDistributedRegionFunctionExecutionOnDataPolicyReplicate_SendException() {
    for (var vm : toArray(empty, normal, replicate1, replicate2, replicate3)) {
      vm.invoke(() -> createCache(getDistributedSystemProperties()));
    }

    empty.invoke(() -> createRegion(DataPolicy.EMPTY));
    normal.invoke(() -> createRegion(DataPolicy.NORMAL));
    for (var vm : toArray(replicate1, replicate2, replicate3)) {
      vm.invoke(() -> createRegion(DataPolicy.REPLICATE));
    }

    empty.invoke(() -> populateRegion(200));

    replicate1.invoke(this::executeResultWithExceptionFunction);
  }

  @Test
  public void testDistributedRegionFunctionExecutionOnDataPolicyReplicate_NoLastResult() {
    for (var vm : toArray(empty, normal, replicate1, replicate2, replicate3)) {
      vm.invoke(() -> createCache(getDistributedSystemProperties()));
    }

    empty.invoke(() -> createRegion(DataPolicy.EMPTY));
    normal.invoke(() -> createRegion(DataPolicy.NORMAL));
    for (var vm : toArray(replicate1, replicate2, replicate3)) {
      vm.invoke(() -> createRegion(DataPolicy.REPLICATE));
    }

    empty.invoke(() -> populateRegion(200));

    replicate1.invoke(this::executeNoLastResultFunction);
  }

  @Test
  public void testDistributedRegionFunctionExecutionWithFunctionInvocationTargetException() {
    for (var vm : toArray(empty, normal, replicate1, replicate2, replicate3)) {
      vm.invoke(() -> createCache(getDistributedSystemProperties()));
    }

    empty.invoke(() -> createRegion(DataPolicy.EMPTY));
    normal.invoke(() -> createRegion(DataPolicy.NORMAL));
    for (var vm : toArray(replicate1, replicate2, replicate3)) {
      vm.invoke(() -> createRegion(DataPolicy.REPLICATE));
    }

    for (var vm : toArray(empty, normal, replicate1, replicate2, replicate3)) {
      vm.invoke(() -> registerThrowsFunctionInvocationTargetExceptionFunction(true, 5));
    }

    empty.invoke(() -> populateRegion(200));

    replicate1.invoke(() -> {
      try (var ie = addIgnoredException("I have been thrown")) {
        executeFunctionFunctionInvocationTargetException();
      }
    });
  }

  @Test
  public void testDistributedRegionFunctionExecutionWithFunctionInvocationTargetException_WithoutHA() {
    for (var vm : toArray(empty, normal, replicate1, replicate2, replicate3)) {
      vm.invoke(() -> createCache(getDistributedSystemProperties()));
    }

    empty.invoke(() -> createRegion(DataPolicy.EMPTY));
    normal.invoke(() -> createRegion(DataPolicy.NORMAL));
    for (var vm : toArray(replicate1, replicate2, replicate3)) {
      vm.invoke(() -> createRegion(DataPolicy.REPLICATE));
    }

    for (var vm : toArray(empty, normal, replicate1, replicate2, replicate3)) {
      vm.invoke(() -> registerThrowsFunctionInvocationTargetExceptionFunction(false, 0));
    }

    empty.invoke(() -> populateRegion(200));

    replicate1.invoke(() -> {
      try (var ie = addIgnoredException("I have been thrown")) {
        executeFunctionFunctionInvocationTargetExceptionWithoutHA();
      }
    });
  }

  @Test
  public void testDistributedRegionFunctionExecutionWithFunctionInvocationTargetExceptionForEmptyDataPolicy() {
    for (var vm : toArray(empty, normal, replicate1, replicate2, replicate3)) {
      vm.invoke(() -> createCache(getDistributedSystemProperties()));
    }

    empty.invoke(() -> createRegion(DataPolicy.EMPTY));
    normal.invoke(() -> createRegion(DataPolicy.NORMAL));
    for (var vm : toArray(replicate1, replicate2, replicate3)) {
      vm.invoke(() -> createRegion(DataPolicy.REPLICATE));
    }

    for (var vm : toArray(empty, normal, replicate1, replicate2, replicate3)) {
      vm.invoke(() -> registerThrowsFunctionInvocationTargetExceptionFunction(true, 5));
    }

    empty.invoke(() -> populateRegion(200));

    empty.invoke(() -> {
      try (var ie = addIgnoredException("I have been thrown")) {
        executeFunctionFunctionInvocationTargetException();
      }
    });
  }

  @Test
  public void testDistributedRegionFunctionExecutionWithFunctionInvocationTargetExceptionForEmptyDataPolicy_WithoutHA() {
    for (var vm : toArray(empty, normal, replicate1, replicate2, replicate3)) {
      vm.invoke(() -> createCache(getDistributedSystemProperties()));
    }

    empty.invoke(() -> createRegion(DataPolicy.EMPTY));
    normal.invoke(() -> createRegion(DataPolicy.NORMAL));
    for (var vm : toArray(replicate1, replicate2, replicate3)) {
      vm.invoke(() -> createRegion(DataPolicy.REPLICATE));
    }

    for (var vm : toArray(empty, normal, replicate1, replicate2, replicate3)) {
      vm.invoke(() -> registerThrowsFunctionInvocationTargetExceptionFunction(false, 0));
    }

    empty.invoke(() -> populateRegion(200));

    empty.invoke(() -> {
      try (var ie = addIgnoredException("I have been thrown")) {
        executeFunctionFunctionInvocationTargetExceptionWithoutHA();
      }
    });
  }

  @Test
  public void testDistributedRegionFunctionExecutionHACacheClosedException() {
    for (var vm : toArray(empty, replicate1)) {
      vm.invoke(() -> createCache(getDistributedSystemProperties()));
    }

    empty.invoke(() -> createRegion(DataPolicy.EMPTY));
    replicate1.invoke(() -> createRegion(DataPolicy.REPLICATE));

    empty.invoke(() -> populateRegion(200));

    replicate2.invoke(() -> {
      createCache(getDistributedSystemProperties());
      createRegion(DataPolicy.REPLICATE);
    });

    empty.invoke(() -> {
      var result = executeDistributedRegionFunction();

      assertThat(result)
          .hasSize(5001)
          .containsOnly(true);
    });
  }

  @Test
  public void testDistributedRegionFunctionExecutionHANodeFailure() throws Exception {
    for (var vm : toArray(empty, replicate1)) {
      vm.invoke(() -> createCache(getDistributedSystemProperties()));
    }

    empty.invoke(() -> createRegion(DataPolicy.EMPTY));
    replicate1.invoke(() -> createRegion(DataPolicy.REPLICATE));

    empty.invoke(() -> populateRegion(200));

    var executeFunctionHaInEmptyVM =
        empty.invokeAsync(this::executeDistributedRegionFunction);

    replicate2.invoke(() -> {
      createCache(getDistributedSystemProperties());
      createRegion(DataPolicy.REPLICATE);
    });

    replicate1.invoke(() -> getCache().close());

    var result = executeFunctionHaInEmptyVM.get();

    assertThat(result)
        .hasSize(5001)
        .containsOnly(true);
  }

  @Test
  public void testDistributedRegionFunctionExecutionOnDataPolicyEmpty_ClientServer() {
    var empty1 = replicate3;
    var empty2 = normal;
    var client = empty;

    for (var vm : toArray(empty2, replicate1, replicate2, empty1)) {
      vm.invoke(() -> createCache(getDistributedSystemProperties()));
    }

    client.invoke(() -> createClientCache());

    int port1 = empty1.invoke(() -> {
      createRegion(DataPolicy.EMPTY);
      return createCacheServer();
    });
    int port2 = empty2.invoke(() -> {
      createRegion(DataPolicy.EMPTY);
      return createCacheServer();
    });

    for (var vm : toArray(replicate1, replicate2)) {
      vm.invoke(() -> createRegion(DataPolicy.REPLICATE));
    }

    client.invoke(() -> {
      createClientRegion(port1, port2);
      populateClientRegion(200);

      executeDistributedRegionFunction();
    });
  }

  @Test
  public void testDistributedRegionFunctionExecutionOnDataPolicyEmpty_ClientServer_SendException() {
    var empty1 = replicate3;
    var empty2 = normal;
    var client = empty;

    for (var vm : toArray(empty2, replicate1, replicate2, empty1)) {
      vm.invoke(() -> createCache(getDistributedSystemProperties()));
    }

    client.invoke(() -> createClientCache());

    int port1 = empty1.invoke(() -> {
      createRegion(DataPolicy.EMPTY);
      return createCacheServer();
    });
    int port2 = empty2.invoke(() -> {
      createRegion(DataPolicy.EMPTY);
      return createCacheServer();
    });

    for (var vm : toArray(replicate1, replicate2)) {
      vm.invoke(() -> createRegion(DataPolicy.REPLICATE));
    }

    client.invoke(() -> {
      createClientRegion(port1, port2);
      populateClientRegion(200);

      executeResultWithExceptionFunction();
    });
  }

  @Test
  public void testDistributedRegionFunctionExecutionOnDataPolicyEmpty_ClientServer_NoLastResult() {
    var empty1 = replicate3;
    var empty2 = normal;
    var client = empty;

    for (var vm : toArray(empty2, replicate1, replicate2, empty1)) {
      vm.invoke(() -> createCache(getDistributedSystemProperties()));
    }

    client.invoke(() -> createClientCache());

    int port1 = empty1.invoke(() -> {
      createRegion(DataPolicy.EMPTY);
      return createCacheServer();
    });
    int port2 = empty2.invoke(() -> {
      createRegion(DataPolicy.EMPTY);
      return createCacheServer();
    });

    for (var vm : toArray(replicate1, replicate2)) {
      vm.invoke(() -> createRegion(DataPolicy.REPLICATE));
    }

    client.invoke(() -> {
      createClientRegion(port1, port2);
      populateClientRegion(200);

      try (var ie1 = addIgnoredException("did not send last result", empty1);
          var ie2 = addIgnoredException("did not send last result", empty2)) {
        executeNoLastResultFunction();
      }
    });
  }

  /**
   * If one server goes down while executing a function, that function should failover to other
   * available server.
   */
  @Test
  public void testServerFailoverWithTwoServerAliveHA() throws InterruptedException {
    var emptyServer1 = replicate1;
    var client = normal;

    for (var vm : toArray(emptyServer1, replicate2, replicate3)) {
      vm.invoke(() -> createCache(getDistributedSystemProperties()));
    }

    int port1 = emptyServer1.invoke(() -> {
      createRegion(DataPolicy.EMPTY);
      return createCacheServer();
    });
    int port2 = replicate2.invoke(() -> {
      createRegion(DataPolicy.REPLICATE);
      return createCacheServer();
    });

    replicate3.invoke(() -> createRegion(DataPolicy.REPLICATE));

    client.invoke(() -> {
      LATCH.set(new CountDownLatch(1));
      createClientCache();
      createClientRegion(port1, port2);
      populateClientRegion(200);
    });

    replicate2.invoke(this::stopServerHA);

    var executeFunctionHaInClientVm =
        client.invokeAsync(this::executeDistributedRegionFunction);

    replicate2.invoke(() -> {
      startServerHA();
      client.invoke(() -> LATCH.get().countDown());
    });

    client.invoke(() -> {
      LATCH.get().await(getTimeout().toMillis(), MILLISECONDS);
    });

    emptyServer1.invoke(this::closeCacheHA);

    var result = executeFunctionHaInClientVm.get();

    assertThat(result)
        .hasSize(5001)
        .containsOnly(true);
  }

  @Test
  public void testDistributedRegionFunctionExecutionOnDataPolicyNormal_ClientServer() {
    var client = empty;
    var normal1 = normal;
    var normal2 = replicate3;
    var empty = replicate2;

    for (var vm : toArray(normal1, replicate1, empty, normal2)) {
      vm.invoke(() -> createCache(getDistributedSystemProperties()));
    }

    client.invoke(() -> createClientCache());

    int port1 = normal1.invoke(() -> {
      createRegion(DataPolicy.NORMAL);
      return createCacheServer();
    });
    int port2 = normal2.invoke(() -> {
      createRegion(DataPolicy.NORMAL);
      return createCacheServer();
    });

    for (var vm : toArray(empty, replicate1)) {
      vm.invoke(() -> createRegion(DataPolicy.EMPTY));
    }

    client.invoke(() -> {
      createClientRegion(port1, port2);
      populateClientRegion(200);

      // add expected exception
      try (var ie = addIgnoredException(FunctionException.class)) {
        var thrown = catchThrowable(this::executeDistributedRegionFunction);

        assertThat(thrown)
            .isInstanceOf(FunctionException.class)
            .hasMessageContaining(
                "Function execution on region with DataPolicy.NORMAL is not supported");
      }
    });
  }

  @Test
  public void testDistributedRegionFunctionExecutionOnDataPolicyReplicate_ClientServer() {
    var client = empty;
    var empty = replicate3;

    for (var vm : toArray(normal, replicate1, replicate2, empty)) {
      vm.invoke(() -> createCache(getDistributedSystemProperties()));
    }

    client.invoke(() -> createClientCache());

    int port1 = replicate1.invoke(() -> {
      createRegion(DataPolicy.REPLICATE);
      return createCacheServer();
    });
    int port2 = replicate2.invoke(() -> {
      createRegion(DataPolicy.REPLICATE);
      return createCacheServer();
    });

    normal.invoke(() -> createRegion(DataPolicy.NORMAL));
    empty.invoke(() -> createRegion(DataPolicy.EMPTY));

    client.invoke(() -> {
      createClientRegion(port1, port2);
      populateClientRegion(200);

      executeUnregisteredFunction();
    });
  }

  @Test
  public void testDistributedRegionFunctionExecutionOnDataPolicyReplicate_ClientServer_WithoutRegister() {
    var client = empty;
    var empty = replicate3;

    for (var vm : toArray(normal, replicate1, replicate2, empty)) {
      vm.invoke(() -> createCache(getDistributedSystemProperties()));
    }

    client.invoke(() -> createClientCache());

    int port1 = replicate1.invoke(() -> {
      createRegion(DataPolicy.REPLICATE);
      return createCacheServer();
    });
    int port2 = replicate2.invoke(() -> {
      createRegion(DataPolicy.REPLICATE);
      return createCacheServer();
    });

    normal.invoke(() -> createRegion(DataPolicy.NORMAL));
    empty.invoke(() -> createRegion(DataPolicy.EMPTY));

    client.invoke(() -> {
      createClientRegion(port1, port2);
      populateClientRegion(200);

      executeDistributedRegionFunction();
    });
  }

  @Test
  public void testDistributedRegionFunctionExecutionOnDataPolicyReplicate_ClientServer_FunctionInvocationTargetException() {
    var client = empty;
    var empty = replicate3;

    for (var vm : toArray(normal, replicate1, replicate2, empty)) {
      vm.invoke(() -> createCache(getDistributedSystemProperties()));
    }

    int port1 = replicate1.invoke(() -> {
      createRegion(DataPolicy.REPLICATE);
      return createCacheServer();
    });
    int port2 = replicate2.invoke(() -> {
      createRegion(DataPolicy.REPLICATE);
      return createCacheServer();
    });

    client.invoke(() -> createClientCache());
    normal.invoke(() -> createRegion(DataPolicy.NORMAL));
    empty.invoke(() -> createRegion(DataPolicy.EMPTY));

    for (var vm : toArray(client, replicate1, replicate2, normal, empty)) {
      vm.invoke(() -> registerThrowsFunctionInvocationTargetExceptionFunction(true, 5));
    }

    client.invoke(() -> {
      createClientRegion(port1, port2);
      populateClientRegion(200);

      // add expected exception to avoid suspect strings
      try (var ie = addIgnoredException("I have been thrown")) {
        executeFunctionFunctionInvocationTargetException_ClientServer();
      }
    });
  }

  @Test
  public void testDistributedRegionFunctionExecutionOnDataPolicyReplicate_ClientServer_FunctionInvocationTargetException_WithoutHA() {
    var client = empty;
    var empty = replicate3;

    for (var vm : toArray(normal, replicate1, replicate2, empty)) {
      vm.invoke(() -> createCache(getDistributedSystemProperties()));
    }

    int port1 = replicate1.invoke(() -> {
      createRegion(DataPolicy.REPLICATE);
      return createCacheServer();
    });
    int port2 = replicate2.invoke(() -> {
      createRegion(DataPolicy.REPLICATE);
      return createCacheServer();
    });

    client.invoke(() -> createClientCache());
    normal.invoke(() -> createRegion(DataPolicy.NORMAL));
    empty.invoke(() -> createRegion(DataPolicy.EMPTY));

    for (var vm : toArray(client, replicate1, replicate2, normal, empty)) {
      vm.invoke(() -> registerThrowsFunctionInvocationTargetExceptionFunction(false, 0));
    }

    client.invoke(() -> {
      createClientRegion(port1, port2);
      populateClientRegion(200);

      // add expected exception to avoid suspect strings
      try (var ie = addIgnoredException("I have been thrown")) {
        executeFunctionFunctionInvocationTargetException_ClientServer_WithoutHA();
      }
    });
  }

  @Test
  public void testDistributedRegionFunctionExecutionOnDataPolicyEmpty_ClientServer_FunctionInvocationTargetException() {
    var client = empty;
    var empty1 = replicate3;
    var empty2 = normal;

    for (var vm : toArray(empty2, replicate1, replicate2, empty1)) {
      vm.invoke(() -> createCache(getDistributedSystemProperties()));
    }

    int port1 = empty1.invoke(() -> {
      createRegion(DataPolicy.EMPTY);
      return createCacheServer();
    });
    int port2 = empty2.invoke(() -> {
      createRegion(DataPolicy.EMPTY);
      return createCacheServer();
    });

    client.invoke(() -> createClientCache());
    for (var vm : toArray(replicate1, replicate2)) {
      vm.invoke(() -> createRegion(DataPolicy.REPLICATE));
    }

    client.invoke(() -> {
      createClientRegion(port1, port2);
      populateClientRegion(200);
    });

    for (var vm : toArray(client, empty1, empty2, replicate1, replicate2)) {
      vm.invoke(() -> registerThrowsFunctionInvocationTargetExceptionFunction(true, 5));
    }

    client.invoke(() -> {
      // add expected exception to avoid suspect strings
      try (var ie = addIgnoredException("I have been thrown")) {
        executeFunctionFunctionInvocationTargetException_ClientServer();
      }
    });
  }

  @Test
  public void testDistributedRegionFunctionExecutionOnDataPolicyEmpty_ClientServer_FunctionInvocationTargetException_WithoutHA() {
    var client = empty;
    var empty1 = replicate3;
    var empty2 = normal;

    for (var vm : toArray(empty2, replicate1, replicate2, empty1)) {
      vm.invoke(() -> createCache(getDistributedSystemProperties()));
    }

    client.invoke(() -> createClientCache());

    int port1 = empty1.invoke(() -> {
      createRegion(DataPolicy.EMPTY);
      return createCacheServer();
    });
    int port2 = empty2.invoke(() -> {
      createRegion(DataPolicy.EMPTY);
      return createCacheServer();
    });

    for (var vm : toArray(replicate1, replicate2)) {
      vm.invoke(() -> createRegion(DataPolicy.REPLICATE));
    }

    client.invoke(() -> {
      createClientRegion(port1, port2);
      populateClientRegion(200);
    });

    for (var vm : toArray(client, empty1, empty2, replicate1, replicate2)) {
      vm.invoke(() -> registerThrowsFunctionInvocationTargetExceptionFunction(false, 0));
    }

    client.invoke(() -> {
      // add expected exception to avoid suspect strings
      try (var ie = addIgnoredException("I have been thrown")) {
        executeFunctionFunctionInvocationTargetException_ClientServer_WithoutHA();
      }
    });
  }

  @Test
  public void inlineFunctionIsUsedOnClientInsteadOfLookingUpFunctionById() {
    var client = empty;
    var empty = replicate3;

    for (var vm : toArray(normal, empty, replicate1, replicate2)) {
      vm.invoke(() -> createCache(getDistributedSystemProperties()));
    }
    client.invoke(() -> createClientCache(getClientProperties()));

    int port1 = replicate1.invoke(() -> {
      createRegion(DataPolicy.REPLICATE);
      return createCacheServer();
    });
    int port2 = replicate2.invoke(() -> {
      createRegion(DataPolicy.REPLICATE);
      return createCacheServer();
    });

    for (var vm : toArray(normal, empty)) {
      vm.invoke(() -> createRegion(DataPolicy.REPLICATE));
    }
    client.invoke(() -> {
      createClientRegion(port1, port2);
      populateClientRegion(200);
    });

    for (var vm : toArray(normal, empty, replicate1, replicate2, client)) {
      vm.invoke(() -> FunctionService.registerFunction(inlineFunction("Failure", false)));
    }

    client.invoke(() -> {
      var resultCollector = TypedFunctionService
          .<Boolean, Boolean, List<Boolean>>onRegion(getRegion())
          .setArguments(true)
          .execute(inlineFunction("Success", true));

      assertThat(resultCollector.getResult())
          .hasSize(1)
          .containsOnly(true);
    });
  }

  /**
   * Verify that AuthenticationRequiredException is not thrown when security-* properties are NOT
   * provided. We have to grep for this exception in logs for any occurrence.
   */
  @Test
  public void authenticationRequiredExceptionIsNotThrownWhenSecurityIsNotConfigured() {
    var client = replicate1;
    var server = replicate2;

    int port = server.invoke(() -> {
      createCacheWithSecurity();
      createRegion(DataPolicy.REPLICATE);
      return createCacheServer();
    });

    client.invoke(() -> {
      createClientCacheWithSecurity();
      createClientRegion(port);

      executeNoResultFunction();
      doPut();
    });
  }

  @Test
  public void testFunctionWithNoResultThrowsException() {
    var client = empty;

    int port1 = replicate1.invoke(() -> {
      createCache(getDistributedSystemProperties());
      createRegion(DataPolicy.REPLICATE);
      return createCacheServer();
    });
    int port2 = replicate2.invoke(() -> {
      createCache(getDistributedSystemProperties());
      createRegion(DataPolicy.REPLICATE);
      return createCacheServer();
    });

    client.invoke(() -> {
      createClientCache();
      createClientRegion(port1, port2);
      populateClientRegion(200);

      try (var ie = addIgnoredException(RuntimeException.class)) {
        executeThrowsRuntimeExceptionFunction();
      }
    });
  }

  private Properties getDistributedSystemProperties() {
    var props = DistributedRule.getDistributedSystemProperties();
    props.setProperty(SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.internal.cache.functions.**;org.apache.geode.internal.cache.execute.**;org.apache.geode.test.dunit.**");
    return props;
  }

  private Properties getClientProperties() {
    var props = new Properties();
    props.setProperty(SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.internal.cache.functions.**;org.apache.geode.internal.cache.execute.**;org.apache.geode.test.dunit.**");
    return props;
  }

  private void createCache(Properties props) {
    cacheRule.createCache(props);
  }

  private void createClientCache(Properties props) {
    clientCacheRule.createClientCache(props);
  }

  private InternalCache getCache() {
    return cacheRule.getCache();
  }

  private InternalClientCache getClientCache() {
    return clientCacheRule.getClientCache();
  }

  private void createCacheWithSecurity() {
    var props = getDistributedSystemProperties();
    props.setProperty(NAME, "SecurityServer");
    props.setProperty(SECURITY_CLIENT_AUTHENTICATOR,
        DummyAuthenticator.class.getName() + ".create");

    createCache(props);
  }

  private void createClientCache() {
    var props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");

    createClientCache(props);
  }

  private void createClientCacheWithSecurity() {
    var props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(NAME, "SecurityClient");
    props.setProperty(SECURITY_CLIENT_AUTH_INIT, UserPasswordAuthInit.class.getName() + ".create");
    props.setProperty("security-username", "reader1");
    props.setProperty("security-password", "reader1");

    createClientCache(props);
  }

  private void createClientRegion(int... ports) {
    System.setProperty(GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints", "true");

    var poolFactory = PoolManager.createFactory();
    for (var port : ports) {
      poolFactory.addServer("localhost", port);
    }
    var pool = poolFactory
        .setMaxConnections(10)
        .setMinConnections(6)
        .setPingInterval(3000)
        .setReadTimeout(2000)
        .setRetryAttempts(2)
        .setSocketBufferSize(1000)
        .setSubscriptionEnabled(false)
        .setSubscriptionRedundancy(-1)
        .create(poolName);

    Region<?, ?> region = getClientCache()
        .createClientRegionFactory(ClientRegionShortcut.PROXY)
        .setPoolName(pool.getName())
        .create(regionName);

    setRegion(region);
  }

  private void createRegion(DataPolicy dataPolicy) {
    Region<?, ?> region = getCache().createRegionFactory()
        .setDataPolicy(dataPolicy)
        .setScope(Scope.DISTRIBUTED_ACK)
        .create(regionName);

    setRegion(region);
  }

  private int createCacheServer() throws IOException {
    var cacheServer = getCache().addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();
    return cacheServer.getPort();
  }

  private void doPut() {
    Region<String, String> region = getRegion();
    region.put("K1", "B1");
  }

  private void populateRegion(int count) {
    Region<String, Integer> region = getRegion();
    for (var i = 1; i <= count; i++) {
      region.put("execKey-" + i, i);
    }
  }

  private void populateClientRegion(int count) {
    Region<String, Integer> region = getRegion();
    for (var i = 1; i <= count; i++) {
      region.put("execKey-" + i, i);
    }
  }

  private void startServerHA() throws IOException {
    for (var cacheServer : getCache().getCacheServers()) {
      cacheServer.start();
    }
  }

  private void stopServerHA() {
    for (var cacheServer : getCache().getCacheServers()) {
      cacheServer.stop();
    }
  }

  private void closeCacheHA() {
    for (var cacheServer : getCache().getCacheServers()) {
      cacheServer.stop();
    }

    getCache().close();
  }

  private void registerThrowsFunctionInvocationTargetExceptionFunction(boolean isHA,
      int retryCount) {
    FunctionService.registerFunction(
        new ThrowsFunctionInvocationTargetExceptionFunction(isHA, retryCount));
  }

  private void executeNoResultFunction() {
    TypedFunctionService
        .onRegion(getRegion())
        .execute(new NoResultFunction());
  }

  private List<Boolean> executeDistributedRegionFunction() {
    return TypedFunctionService
        .<Boolean, Boolean, List<Boolean>>onRegion(getRegion())
        .withFilter(filter)
        .setArguments(false)
        .execute(new DistributedRegionFunction())
        .getResult();
  }

  private void executeThrowsRuntimeExceptionFunction() {
    TypedFunctionService
        .<Void, Void, Void>onRegion(getRegion())
        .withFilter(filter)
        .execute(new ThrowsRuntimeExceptionFunction());
  }

  private void executeResultWithExceptionFunction() {
    Function function = new ResultWithExceptionFunction();

    Set<String> filter = new HashSet<>();
    for (var i = 0; i <= 19; i++) {
      filter.add("execKey-" + 100 + i);
    }

    var resultCollector = TypedFunctionService
        .<Boolean, Object, List<Object>>onRegion(getRegion())
        .withFilter(filter)
        .setArguments(true)
        .execute(function);

    var result = resultCollector.getResult();
    result.sort(new NumericComparator());

    assertThat(result.get(0))
        .as("First element of " + resultCollector.getResult())
        .isInstanceOf(CustomRuntimeException.class);

    resultCollector = TypedFunctionService
        .<Set<String>, Object, List<Object>>onRegion(getRegion())
        .withFilter(filter)
        .setArguments(filter)
        .execute(function);

    result = resultCollector.getResult();
    result.sort(new NumericComparator());

    assertThat(result)
        .hasSize(filter.size() + 1)
        .containsAll(IntStream.rangeClosed(0, 19).boxed().collect(toList()));

    assertThat(result.get(result.size() - 1))
        .as("Last element of " + result)
        .isInstanceOf(CustomRuntimeException.class);
  }

  private void executeNoLastResultFunction() {
    var thrown = catchThrowable(() -> {
      TypedFunctionService
          .onRegion(getRegion())
          .withFilter(filter)
          .execute(new NoLastResultFunction())
          .getResult();
    });

    assertThat(thrown)
        .hasMessageContaining("did not send last result");
  }

  private void executeUnregisteredFunction() {
    FunctionService.unregisterFunction(new DistributedRegionFunction().getId());

    TypedFunctionService
        .<Void, Boolean, List<Boolean>>onRegion(getRegion())
        .withFilter(filter)
        .execute(new DistributedRegionFunction())
        .getResult();
  }

  private void executeFunctionFunctionInvocationTargetException() {
    var resultCollector = TypedFunctionService
        .<Boolean, Integer, List<Integer>>onRegion(getRegion())
        .setArguments(true)
        .execute(ThrowsFunctionInvocationTargetExceptionFunction.class.getSimpleName());

    assertThat(resultCollector.getResult())
        .containsOnly(5);
  }

  private void executeFunctionFunctionInvocationTargetExceptionWithoutHA() {
    var thrown = catchThrowable(() -> {
      TypedFunctionService
          .<Boolean, Integer, List<Integer>>onRegion(getRegion())
          .setArguments(true)
          .execute(ThrowsFunctionInvocationTargetExceptionFunction.class.getSimpleName())
          .getResult();
    });

    assertThat(thrown)
        .isInstanceOf(FunctionException.class);

    assertThat(thrown.getCause())
        .isInstanceOf(FunctionInvocationTargetException.class);
  }

  private void executeFunctionFunctionInvocationTargetException_ClientServer() {
    var resultCollector = TypedFunctionService
        .<Boolean, Integer, List<Integer>>onRegion(getRegion())
        .setArguments(true)
        .execute(ThrowsFunctionInvocationTargetExceptionFunction.class.getSimpleName());

    assertThat(resultCollector.getResult())
        .containsOnly(5);
  }

  private void executeFunctionFunctionInvocationTargetException_ClientServer_WithoutHA() {
    var thrown = catchThrowable(() -> {
      TypedFunctionService
          .<Boolean, Integer, List<Integer>>onRegion(getRegion())
          .setArguments(true)
          .execute(ThrowsFunctionInvocationTargetExceptionFunction.class.getSimpleName())
          .getResult();
    });

    assertThat(thrown)
        .isInstanceOf(FunctionException.class);

    assertThat(thrown.getCause())
        .isInstanceOf(FunctionInvocationTargetException.class);
  }

  private static <K, V> Region<K, V> getRegion() {
    return uncheckedCast(REGION.get());
  }

  private static void setRegion(Region<?, ?> region) {
    REGION.set(region);
  }

  private static Function<Object> inlineFunction(String stringResult, boolean booleanResult) {
    return new Function<Object>() {
      @Override
      public void execute(FunctionContext<Object> context) {
        if (context.getArguments() instanceof String) {
          context.getResultSender().lastResult(stringResult);
        } else if (context.getArguments() instanceof Boolean) {
          context.getResultSender().lastResult(booleanResult);
        }
      }

      @Override
      public String getId() {
        return "Function";
      }
    };
  }

  /**
   * Return Integers provided as Arguments and a last result of CustomRuntimeException.
   */
  private static class ResultWithExceptionFunction implements Function<Object> {

    @Override
    public void execute(FunctionContext<Object> context) {
      if (context.getArguments() instanceof Set) {
        Set<Integer> arguments =
            uncheckedCast(context.getArguments());
        for (var i = 0; i < arguments.size(); i++) {
          context.getResultSender().sendResult(i);
        }
      }
      context.getResultSender().sendException(
          new CustomRuntimeException("I have been thrown from TestFunction with set"));
    }

    @Override
    public String getId() {
      return getClass().getName();
    }

    @Override
    public boolean isHA() {
      return false;
    }
  }

  /**
   * Throw RuntimeException without sending any results.
   */
  private static class ThrowsRuntimeExceptionFunction implements Function<Void> {

    @Override
    public void execute(FunctionContext<Void> context) {
      throw new RuntimeException("failure");
    }

    @Override
    public String getId() {
      return getClass().getName();
    }

    @Override
    public boolean hasResult() {
      return false;
    }

    @Override
    public boolean isHA() {
      return false;
    }
  }

  /**
   * Return the Integer 5 or throw FunctionInvocationTargetException depending on object state.
   */
  private static class ThrowsFunctionInvocationTargetExceptionFunction implements Function<Void> {

    private final AtomicInteger count = new AtomicInteger();
    private final int retryCount;
    private final boolean isHA;

    private ThrowsFunctionInvocationTargetExceptionFunction(boolean isHA, int retryCount) {
      this.isHA = isHA;
      this.retryCount = retryCount;
    }

    @Override
    public void execute(FunctionContext<Void> context) {
      count.incrementAndGet();
      if (retryCount != 0 && count.get() >= retryCount) {
        context.getResultSender().lastResult(5);
      } else {
        throw new FunctionInvocationTargetException("I have been thrown from " + getId());
      }
    }

    @Override
    public String getId() {
      return getClass().getSimpleName();
    }

    @Override
    public boolean isHA() {
      return isHA;
    }
  }

  /**
   * Validate DataPolicy and Filter state, perform Region put operations, and then return 5000
   * boolean true values and a last result of boolean false.
   */
  private static class DistributedRegionFunction implements Function<Boolean> {

    @Override
    public void execute(FunctionContext<Boolean> context) {
      var regionFunctionContext = (RegionFunctionContext) context;
      var region = regionFunctionContext.getDataSet();
      var sys = InternalDistributedSystem.getConnectedInstance();

      assertThat(region.getAttributes().getDataPolicy().withStorage()).isTrue();
      assertThat(region.getAttributes().getDataPolicy()).isNotEqualTo(DataPolicy.NORMAL);
      assertThat(regionFunctionContext.getFilter()).hasSize(20);

      // argument true indicates that CacheClose has to be done from the body itself
      if (context.getArguments() != null && context.getArguments()) {
        // do not close cache in retry
        if (!regionFunctionContext.isPossibleDuplicate()) {
          sys.disconnect();
          throw new CacheClosedException(
              "Throwing CacheClosedException to simulate failover during function exception");
        }
      }

      // intentionally doing region operation to cause cacheClosedException
      region.put("execKey-201", 201);

      if (regionFunctionContext.isPossibleDuplicate()) {
        // Below operation is done when the function is reexecuted
        region.put("execKey-202", 202);
        region.put("execKey-203", 203);
      }

      for (var i = 0; i < 5000; i++) {
        context.getResultSender().sendResult(true);
      }
      context.getResultSender().lastResult(true);
    }

    @Override
    public String getId() {
      return getClass().getSimpleName();
    }
  }

  /**
   * Execute until the provided CountDownLatch counts down to zero.
   */
  private static class LongRunningFunction implements Function<Void> {

    private final CountDownLatch latch;

    private LongRunningFunction(CountDownLatch latch) {
      this.latch = latch;
    }

    @Override
    public void execute(FunctionContext<Void> context) {
      try {
        latch.await(getTimeout().toMillis(), MILLISECONDS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      context.getResultSender().lastResult("LongRunningFunction completed");
    }

    @Override
    public String getId() {
      return getClass().getSimpleName();
    }
  }

  /**
   * Complete immediately with no results.
   */
  private static class NoResultFunction implements Function<Void> {

    @Override
    public void execute(FunctionContext<Void> context) {
      // no result
    }

    @Override
    public String getId() {
      return getClass().getSimpleName();
    }

    @Override
    public boolean hasResult() {
      return false;
    }

    @Override
    public boolean isHA() {
      return false;
    }
  }

  /**
   * Return Arguments as results without a last result.
   */
  private static class NoLastResultFunction implements Function<Object> {

    @Override
    public void execute(FunctionContext<Object> context) {
      context.getResultSender().sendResult(context.getArguments());
    }

    @Override
    public String getId() {
      return getClass().getSimpleName();
    }
  }

  /**
   * Custom RuntimeException.
   */
  private static class CustomRuntimeException extends RuntimeException {

    private CustomRuntimeException(String message) {
      super(message);
    }
  }

  /**
   * Use natural ordering for Integers and moves optional Exception to end of Collection.
   */
  private static class NumericComparator implements Comparator<Object>, Serializable {

    @Override
    public int compare(Object o1, Object o2) {
      if (o1 == o2) {
        return 0;
      }
      if (!(o1 instanceof Integer)) {
        return 1;
      }
      if (!(o2 instanceof Integer)) {
        return -1;
      }
      if ((int) o1 > (int) o2) {
        return 1;
      }
      return -1;
    }
  }
}
