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

import static org.apache.geode.cache.CacheFactory.getAnyInstance;
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.internal.cache.execute.InternalFunctionService.onServer;
import static org.apache.geode.internal.cache.execute.InternalFunctionService.onServers;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.Host.getLocator;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.LogWriterUtils.getDUnitLogLevel;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.test.dunit.Wait.pause;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.FunctionServiceTest;

@Category({FunctionServiceTest.class})
public class OnGroupsFunctionExecutionDUnitTest extends JUnit4DistributedTestCase {

  @Override
  public final void preTearDown() throws Exception {
    Invoke.invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Cache c = null;
        try {
          c = CacheFactory.getAnyInstance();
          if (c != null) {
            c.close();
          }
        } catch (CacheClosedException ignored) {
        }
        return null;
      }
    });
  }

  static class OnGroupsFunction extends FunctionAdapter implements DataSerializable {
    private static final long serialVersionUID = -1032915440862585532L;
    public static final String Id = "OnGroupsFunction";
    public static int invocationCount;

    public OnGroupsFunction() {}

    @Override
    public void execute(FunctionContext context) {
      LogWriterUtils.getLogWriter().fine("SWAP:1:executing OnGroupsFunction:" + invocationCount);
      var ds = InternalDistributedSystem.getConnectedInstance();
      synchronized (OnGroupsFunction.class) {
        invocationCount++;
      }
      var l = (ArrayList<String>) context.getArguments();
      if (l != null) {
        assertFalse(Collections.disjoint(l, ds.getDistributedMember().getGroups()));
      }
      if (hasResult()) {
        context.getResultSender().lastResult(Boolean.TRUE);
      }
    }

    @Override
    public String getId() {
      return Id;
    }

    @Override
    public void toData(DataOutput out) throws IOException {

    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {

    }
  }

  private void initVM(VM vm, final String groups, final String regionName,
      final boolean startServer) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var props = new Properties();
        props.put(GROUPS, groups);
        if (regionName != null) {
          Cache c = null;
          try {
            c = CacheFactory.getInstance(getSystem(props));
            c.close();
          } catch (CacheClosedException ignored) {
          }
          c = CacheFactory.create(getSystem(props));
          c.createRegionFactory(RegionShortcut.PARTITION).create(regionName);
          if (startServer) {
            var s = c.addCacheServer();
            s.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
            s.start();
          }
        } else {
          getSystem(props);
        }
        return null;
      }
    });
  }

  private void registerFunction(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        FunctionService.registerFunction(new OnGroupsFunction());
        return null;
      }
    });
  }

  private void verifyAndResetInvocationCount(VM vm, final int count) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var f = (OnGroupsFunction) FunctionService.getFunction(OnGroupsFunction.Id);

        // assert succeeded, reset count
        synchronized (OnGroupsFunction.class) {
          assertEquals(count, OnGroupsFunction.invocationCount);
          OnGroupsFunction.invocationCount = 0;
        }
        return null;
      }
    });
  }

  private int getAndResetInvocationCount(VM vm) {
    return (Integer) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var f = (OnGroupsFunction) FunctionService.getFunction(OnGroupsFunction.Id);
        var count = 0;
        synchronized (OnGroupsFunction.class) {
          count = OnGroupsFunction.invocationCount;
          OnGroupsFunction.invocationCount = 0;
        }
        return count;
      }
    });
  }

  private int getInvocationCount(VM vm) {
    return (Integer) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var f = (OnGroupsFunction) FunctionService.getFunction(OnGroupsFunction.Id);
        var count = 0;
        synchronized (OnGroupsFunction.class) {
          count = OnGroupsFunction.invocationCount;
        }

        return count;
      }
    });
  }

  private void resetInvocationCount(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var f = (OnGroupsFunction) FunctionService.getFunction(OnGroupsFunction.Id);
        synchronized (OnGroupsFunction.class) {
          OnGroupsFunction.invocationCount = 0;
        }
        return null;
      }
    });
  }

  @Test
  public void testBasicP2PFunctionNoCache() {
    doBasicP2PFunctionNoCache(false);
  }

  @Test
  public void testBasicP2pRegisteredFunctionNoCache() {
    doBasicP2PFunctionNoCache(true);
  }

  private void doBasicP2PFunctionNoCache(final boolean registerFunction) {
    var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);

    initVM(vm0, "g0,gm", null, false);
    initVM(vm1, "g1", null, false);
    initVM(vm2, "g0,g1", null, false);

    if (registerFunction) {
      registerFunction(vm0);
      registerFunction(vm1);
      registerFunction(vm2);
    }

    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        LogWriterUtils.getLogWriter().fine("SWAP:invoking on gm");
        DistributedSystem ds = getSystem();
        try {
          FunctionService.onMember("no such group");
          fail("expected exception not thrown");
        } catch (FunctionException ignored) {
        }
        var e = FunctionService.onMembers("gm");
        var args = new ArrayList<String>();
        args.add("gm");
        e = e.setArguments(args);
        if (registerFunction) {
          e.execute(OnGroupsFunction.Id).getResult();
        } else {
          e.execute(new OnGroupsFunction()).getResult();
        }
        return null;
      }
    });
    verifyAndResetInvocationCount(vm0, 1);
    verifyAndResetInvocationCount(vm1, 0);
    verifyAndResetInvocationCount(vm2, 0);

    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        DistributedSystem ds = getSystem();
        LogWriterUtils.getLogWriter().fine("SWAP:invoking on g0");
        var e = FunctionService.onMembers("g0");
        var args = new ArrayList<String>();
        args.add("g0");
        e = e.setArguments(args);
        if (registerFunction) {
          e.execute(OnGroupsFunction.Id).getResult();
        } else {
          e.execute(new OnGroupsFunction()).getResult();
        }
        return null;
      }
    });
    verifyAndResetInvocationCount(vm0, 1);
    verifyAndResetInvocationCount(vm1, 0);
    verifyAndResetInvocationCount(vm2, 1);

    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        DistributedSystem ds = getSystem();
        var e = FunctionService.onMembers("g1");
        var args = new ArrayList<String>();
        args.add("g1");
        e = e.setArguments(args);
        if (registerFunction) {
          e.execute(OnGroupsFunction.Id).getResult();
        } else {
          e.execute(new OnGroupsFunction()).getResult();
        }
        return null;
      }
    });
    verifyAndResetInvocationCount(vm0, 0);
    verifyAndResetInvocationCount(vm1, 1);
    verifyAndResetInvocationCount(vm2, 1);

    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        LogWriterUtils.getLogWriter().fine("SWAP:invoking on g0 g1");
        var ds = InternalDistributedSystem.getConnectedInstance();
        var e = FunctionService.onMembers("g0", "g1");
        var args = new ArrayList<String>();
        args.add("g0");
        args.add("g1");
        e = e.setArguments(args);
        if (registerFunction) {
          e.execute(OnGroupsFunction.Id).getResult();
        } else {
          e.execute(new OnGroupsFunction()).getResult();
        }
        return null;
      }
    });
    verifyAndResetInvocationCount(vm0, 1);
    verifyAndResetInvocationCount(vm1, 1);
    verifyAndResetInvocationCount(vm2, 1);
  }

  @Test
  public void testonMember() {
    var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);

    initVM(vm0, "g0,gm", null, false);
    initVM(vm1, "g1", null, false);
    initVM(vm2, "g0,g1", null, false);

    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        DistributedSystem ds = getSystem();
        try {
          FunctionService.onMember("no such group");
          fail("expected exception not thrown");
        } catch (FunctionException ignored) {
        }
        try {
          FunctionService.onMember();
          fail("expected exception not thrown");
        } catch (FunctionException ignored) {
        }
        FunctionService.onMember("g1").execute(new OnGroupsFunction()).getResult();
        return null;
      }
    });
    var c0 = getAndResetInvocationCount(vm0);
    var c1 = getAndResetInvocationCount(vm1);
    var c2 = getAndResetInvocationCount(vm2);
    assertEquals(1, c0 + c1 + c2);

    // test that function is invoked locally when this member belongs to group
    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        DistributedSystem ds = getSystem();
        FunctionService.onMember("g0").execute(new OnGroupsFunction()).getResult();
        return null;
      }
    });
    verifyAndResetInvocationCount(vm0, 1);
    verifyAndResetInvocationCount(vm1, 0);
    verifyAndResetInvocationCount(vm2, 0);
  }

  static class OnGroupMultiResultFunction extends FunctionAdapter implements DataSerializable {
    private static final long serialVersionUID = 8190290175486881994L;
    public static final String Id = "OnGroupMultiResultFunction";

    public OnGroupMultiResultFunction() {}

    @Override
    public void execute(FunctionContext context) {
      // send 5 1s
      for (var i = 0; i < 4; i++) {
        context.getResultSender().sendResult(1);
      }
      context.getResultSender().lastResult(1);
    }

    @Override
    public String getId() {
      return Id;
    }

    @Override
    public void toData(DataOutput out) throws IOException {

    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {

    }
  }

  @Test
  public void testBasicP2PFunction() {
    var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);
    final var regionName = getName();

    initVM(vm0, "g0,mg", regionName, false);
    initVM(vm1, "g1", regionName, false);
    initVM(vm2, "g0,g1", regionName, false);

    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        DistributedSystem ds = getSystem();
        var e = FunctionService.onMembers("mg");
        var l =
            (ArrayList<Integer>) e.execute(new OnGroupMultiResultFunction()).getResult();
        var sum = 0;
        for (var integer : l) {
          sum += integer;
        }
        assertEquals(5, sum);
        return null;
      }
    });

    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        DistributedSystem ds = getSystem();
        var e = FunctionService.onMembers("g0");
        var l =
            (ArrayList<Integer>) e.execute(new OnGroupMultiResultFunction()).getResult();
        var sum = 0;
        for (var integer : l) {
          sum += integer;
        }
        assertEquals(10, sum);
        return null;
      }
    });

    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        DistributedSystem ds = getSystem();
        var e = FunctionService.onMembers("g0", "g1");
        var l =
            (ArrayList<Integer>) e.execute(new OnGroupMultiResultFunction()).getResult();
        var sum = 0;
        for (var integer : l) {
          sum += integer;
        }
        assertEquals(15, sum);
        return null;
      }
    });
  }

  private int getLocatorPort(VM locator) {
    return (Integer) locator.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        return Locator.getLocator().getPort();
      }
    });
  }

  static class OnGroupsExceptionFunction extends FunctionAdapter implements DataSerializable {
    private static final long serialVersionUID = 6488843931404616442L;
    public static final String Id = "OnGroupsExceptionFunction";

    public OnGroupsExceptionFunction() {}

    @Override
    public void execute(FunctionContext context) {
      var args = (ArrayList<String>) context.getArguments();
      if (args.get(0).equals("runtime")) {
        if (args.size() > 1) {
          var group = args.get(1);
          var ds = InternalDistributedSystem.getConnectedInstance();
          if (ds.getDistributedMember().getGroups().contains(group)) {
            throw new NullPointerException();
          }
        } else {
          throw new NullPointerException();
        }
      } else {
        var ds = InternalDistributedSystem.getConnectedInstance();
        if (args.size() > 1) {
          var group = args.get(1);
          if (ds.getDistributedMember().getGroups().contains(group)) {
            ds.disconnect();
          }
        } else {
          ds.disconnect();
        }
      }
      context.getResultSender().lastResult(Boolean.TRUE);
    }

    @Override
    public String getId() {
      return Id;
    }

    @Override
    public void toData(DataOutput out) throws IOException {

    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {

    }
  }

  @Test
  public void testP2PException() {
    var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);
    final var regionName = getName();

    // The test function deliberately throws a null pointer exception.
    // which is logged.
    IgnoredException.addIgnoredException(NullPointerException.class.getSimpleName());

    initVM(vm0, "g0,mg", regionName, false);
    initVM(vm1, "g1", regionName, false);
    initVM(vm2, "g0,g1,g2", regionName, false);

    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        DistributedSystem ds = getSystem();
        var e = FunctionService.onMembers("mg");
        var args = new ArrayList<String>();
        args.add("runtime");
        e = e.setArguments(args);
        try {
          e.execute(new OnGroupsExceptionFunction()).getResult();
          fail("expected exception not thrown");
        } catch (FunctionException ex) {
          assertTrue(ex.getCause() instanceof NullPointerException);
        }

        var e1 = FunctionService.onMembers("g1");
        e1 = e1.setArguments(args);
        try {
          e1.execute(new OnGroupsExceptionFunction()).getResult();
          fail("expected exception not thrown");
        } catch (FunctionException ex) {
          assertTrue(ex.getCause() instanceof NullPointerException);
        }

        // fail on only one member
        var e2 = FunctionService.onMembers("g1");
        args.add("g2");
        e2 = e2.setArguments(args);
        try {
          e2.execute(new OnGroupsExceptionFunction()).getResult();
          fail("expected exception not thrown");
        } catch (FunctionException ex) {
          assertTrue(ex.getCause() instanceof NullPointerException);
        }
        return null;
      }
    });
  }

  @Test
  public void testP2PMemberFailure() {
    var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);
    final var regionName = getName();

    initVM(vm0, "g0,mg", regionName, false);
    initVM(vm1, "g1", regionName, false);
    initVM(vm2, "g0,g1,g2", regionName, false);

    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        DistributedSystem ds = getSystem();
        var e1 = FunctionService.onMembers("g1");
        var args = new ArrayList<String>();
        args.add("shutdown");
        e1 = e1.setArguments(args);
        try {
          e1.execute(new OnGroupsExceptionFunction()).getResult();
          fail("expected exception not thrown");
        } catch (FunctionException ex) {
          assertTrue(ex.getCause() instanceof FunctionInvocationTargetException);
        }
        return null;
      }
    });
  }

  @Test
  public void testP2POneMemberFailure() {
    var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);
    final var regionName = getName();

    initVM(vm0, "g0,mg", regionName, false);
    initVM(vm1, "g1", regionName, false);
    initVM(vm2, "g0,g1,g2", regionName, false);

    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        DistributedSystem ds = getSystem();
        var e1 = FunctionService.onMembers("g1");
        var args = new ArrayList<String>();
        args.add("shutdown");
        args.add("g2");
        e1 = e1.setArguments(args);
        try {
          e1.execute(new OnGroupsExceptionFunction()).getResult();
          fail("expected exception not thrown");
        } catch (FunctionException ex) {
          assertTrue(ex.getCause() instanceof FunctionInvocationTargetException);
        }
        return null;
      }
    });
  }

  @Test
  public void testP2PIgnoreMemberFailure() {
    var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);
    final var regionName = getName();

    initVM(vm0, "g0,mg", regionName, false);
    initVM(vm1, "g1", regionName, false);
    initVM(vm2, "g0,g1,g2", regionName, false);

    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        DistributedSystem ds = getSystem();
        var e1 = FunctionService.onMembers("g1");
        var args = new ArrayList<String>();
        args.add("shutdown");
        args.add("g2");
        e1 = e1.setArguments(args);
        ((AbstractExecution) e1).setIgnoreDepartedMembers(true);
        var l = (ArrayList) e1.execute(new OnGroupsExceptionFunction()).getResult();
        assertEquals(2, l.size());
        if (l.get(0) instanceof FunctionInvocationTargetException) {
          assertTrue((Boolean) l.get(1));
        } else if (l.get(0) instanceof Boolean) {
          assertTrue(l.get(1) instanceof FunctionInvocationTargetException);
        } else {
          fail("expected to find a Boolean or throwable at index 0");
        }
        return null;
      }
    });
  }

  @Test
  public void testBasicClientServerFunction() {
    dotestBasicClientServerFunction(false, true);
  }

  @Test
  public void testBasicClientServerRegisteredFunction() {
    dotestBasicClientServerFunction(true, true);
  }

  @Test
  public void testBasicClientServerFunctionNoArgs() {
    dotestBasicClientServerFunction(false, false);
  }

  @Test
  public void testBasicClientServerRegisteredFunctionNoArgs() {
    dotestBasicClientServerFunction(true, false);
  }

  private void dotestBasicClientServerFunction(final boolean register, final boolean withArgs) {
    var host = Host.getHost(0);
    var server0 = host.getVM(0);
    var server1 = host.getVM(1);
    var server2 = host.getVM(2);
    var client = host.getVM(3);
    var locator = Host.getLocator();
    final var regionName = getName();

    initVM(server0, "mg,g0", regionName, true);
    initVM(server1, "g1", regionName, true);
    initVM(server2, "g0,g1", regionName, true);

    if (register) {
      registerFunction(server0);
      registerFunction(server1);
      registerFunction(server2);
    }

    final var locatorPort = getLocatorPort(locator);
    final var hostName = host.getHostName();

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          var c = CacheFactory.getAnyInstance();
          c.close();
        } catch (CacheClosedException ignored) {
        }
        disconnectFromDS();
        LogWriterUtils.getLogWriter().fine("SWAP:creating client cache");
        var ccf = new ClientCacheFactory();
        ccf.addPoolLocator(hostName, locatorPort);
        ccf.setPoolServerGroup("mg");
        ccf.set(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
        var c = ccf.create();

        c.getLogger().info("SWAP:invoking function from client on g0");
        var e = InternalFunctionService.onServers(c, "g0");
        if (withArgs) {
          var args = new ArrayList<String>();
          args.add("g0");
          e = e.setArguments(args);
        }
        if (register) {
          e.execute(OnGroupsFunction.Id).getResult();
        } else {
          e.execute(new OnGroupsFunction()).getResult();
        }
        return null;
      }
    });
    verifyAndResetInvocationCount(server0, 1);
    verifyAndResetInvocationCount(server1, 0);
    verifyAndResetInvocationCount(server2, 1);

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var c = ClientCacheFactory.getAnyInstance();
        c.getLogger().fine("SWAP:invoking function from client on mg");
        var e = InternalFunctionService.onServers(c, "mg");
        if (withArgs) {
          var args = new ArrayList<String>();
          args.add("mg");
          e = e.setArguments(args);
        }
        if (register) {
          e.execute(OnGroupsFunction.Id).getResult();
        } else {
          e.execute(new OnGroupsFunction()).getResult();
        }
        return null;
      }
    });
    verifyAndResetInvocationCount(server0, 1);
    verifyAndResetInvocationCount(server1, 0);
    verifyAndResetInvocationCount(server2, 0);

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var c = ClientCacheFactory.getAnyInstance();
        c.getLogger().fine("SWAP:invoking function from client on g0 g1");
        var e = InternalFunctionService.onServers(c, "g0", "g1");
        if (withArgs) {
          var args = new ArrayList<String>();
          args.add("g0");
          args.add("g1");
          e = e.setArguments(args);
        }
        if (register) {
          e.execute(OnGroupsFunction.Id).getResult();
        } else {
          e.execute(new OnGroupsFunction()).getResult();
        }
        return null;
      }
    });
    verifyAndResetInvocationCount(server0, 1);
    verifyAndResetInvocationCount(server1, 1);
    verifyAndResetInvocationCount(server2, 1);
  }

  @Test
  public void testStreamingClientServerFunction() {
    var host = Host.getHost(0);
    var server0 = host.getVM(0);
    var server1 = host.getVM(1);
    var server2 = host.getVM(2);
    var client = host.getVM(3);
    var locator = Host.getLocator();
    final var regionName = getName();

    initVM(server0, "mg,g0", regionName, true);
    initVM(server1, "g1", regionName, true);
    initVM(server2, "g0,g1", regionName, true);

    final var locatorPort = getLocatorPort(locator);
    final var hostName = host.getHostName();

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          var c = CacheFactory.getAnyInstance();
          c.close();
        } catch (CacheClosedException ignored) {
        }
        disconnectFromDS();
        LogWriterUtils.getLogWriter().fine("SWAP:creating client cache");
        var ccf = new ClientCacheFactory();
        ccf.addPoolLocator(hostName, locatorPort);
        ccf.setPoolServerGroup("mg");
        ccf.set(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
        var c = ccf.create();

        c.getLogger().info("SWAP:invoking function from client on g0");
        var e = InternalFunctionService.onServers(c, "g0");
        var l =
            (ArrayList<Integer>) e.execute(new OnGroupMultiResultFunction()).getResult();
        var sum = 0;
        for (var integer : l) {
          sum += integer;
        }
        assertEquals(10, sum);
        return null;
      }
    });

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var c = ClientCacheFactory.getAnyInstance();
        c.getLogger().fine("SWAP:invoking function from client on mg");
        var e = InternalFunctionService.onServers(c, "mg");
        var l =
            (ArrayList<Integer>) e.execute(new OnGroupMultiResultFunction()).getResult();
        var sum = 0;
        for (var integer : l) {
          sum += integer;
        }
        assertEquals(5, sum);
        return null;
      }
    });

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var c = ClientCacheFactory.getAnyInstance();
        c.getLogger().fine("SWAP:invoking function from client on g0 g1");
        var e = InternalFunctionService.onServers(c, "g0", "g1");
        var l =
            (ArrayList<Integer>) e.execute(new OnGroupMultiResultFunction()).getResult();
        var sum = 0;
        for (var integer : l) {
          sum += integer;
        }
        assertEquals(15, sum);
        return null;
      }
    });
  }

  @Test
  public void testOnServer() {
    var host = Host.getHost(0);
    var server0 = host.getVM(0);
    var server1 = host.getVM(1);
    var server2 = host.getVM(2);
    var client = host.getVM(3);
    var locator = Host.getLocator();
    final var regionName = getName();

    initVM(server0, "mg,g0", regionName, true);
    initVM(server1, "g1", regionName, true);
    initVM(server2, "g0,g1,g2", regionName, true);

    final var locatorPort = getLocatorPort(locator);
    final var hostName = host.getHostName();

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          var c = CacheFactory.getAnyInstance();
          c.close();
        } catch (CacheClosedException ignored) {
        }
        disconnectFromDS();
        LogWriterUtils.getLogWriter().fine("SWAP:creating client cache");
        var ccf = new ClientCacheFactory();
        ccf.addPoolLocator(hostName, locatorPort);
        ccf.setPoolServerGroup("mg");
        ccf.set(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
        var c = ccf.create();

        var ex = IgnoredException.addIgnoredException("No member found");
        try {
          InternalFunctionService.onServer(c, "no such group").execute(new OnGroupsFunction())
              .getResult();
          fail("expected exception not thrown");
        } catch (FunctionException ignored) {
        } finally {
          ex.remove();
        }

        InternalFunctionService.onServer(c, "g1").execute(new OnGroupsFunction()).getResult();
        return null;
      }
    });
    var c0 = getAndResetInvocationCount(server0);
    var c1 = getAndResetInvocationCount(server1);
    var c2 = getAndResetInvocationCount(server2);
    assertEquals(1, c0 + c1 + c2);

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var c = ClientCacheFactory.getAnyInstance();
        InternalFunctionService.onServer(c, "g0").execute(new OnGroupsFunction()).getResult();

        return null;
      }
    });

    verifyAndResetInvocationCount(server0, 1);
    verifyAndResetInvocationCount(server1, 0);
    verifyAndResetInvocationCount(server2, 0);

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var c = ClientCacheFactory.getAnyInstance();
        InternalFunctionService.onServer(c, "mg", "g1").execute(new OnGroupsFunction()).getResult();

        return null;
      }
    });

    c0 = getAndResetInvocationCount(server0);
    c1 = getAndResetInvocationCount(server1);
    c2 = getAndResetInvocationCount(server2);
    assertEquals(2, c0 + c1 + c2);
  }

  @Test
  public void testClientServerException() {
    var host = Host.getHost(0);
    var server0 = host.getVM(0);
    var server1 = host.getVM(1);
    var server2 = host.getVM(2);
    var client = host.getVM(3);
    var locator = Host.getLocator();
    final var regionName = getName();

    initVM(server0, "mg,g0", regionName, true);
    initVM(server1, "g1", regionName, true);
    initVM(server2, "g0,g1,g2", regionName, true);

    final var locatorPort = getLocatorPort(locator);
    final var hostName = host.getHostName();

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          var c = CacheFactory.getAnyInstance();
          c.close();
        } catch (CacheClosedException ignored) {
        }
        disconnectFromDS();
        LogWriterUtils.getLogWriter().fine("SWAP:creating client cache");
        var ccf = new ClientCacheFactory();
        ccf.addPoolLocator(hostName, locatorPort);
        ccf.setPoolServerGroup("mg");
        ccf.set(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
        var c = ccf.create();

        var expected = IgnoredException.addIgnoredException("No member found");
        try {
          InternalFunctionService.onServers(c, "no such group").execute(new OnGroupsFunction())
              .getResult();
          fail("expected exception not thrown");
        } catch (FunctionException ignored) {
        } finally {
          expected.remove();
        }

        IgnoredException.addIgnoredException("NullPointerException");
        var e = InternalFunctionService.onServers(c, "mg");
        var args = new ArrayList<String>();
        args.add("runtime");
        e = e.setArguments(args);
        try {
          e.execute(new OnGroupsExceptionFunction()).getResult();
          fail("expected exception not thrown");
        } catch (FunctionException ex) {
          assertTrue(ex.getCause() instanceof NullPointerException);
        }

        var e1 = InternalFunctionService.onServers(c, "g1");
        e1 = e1.setArguments(args);
        try {
          e1.execute(new OnGroupsExceptionFunction()).getResult();
          fail("expected exception not thrown");
        } catch (FunctionException ex) {
          assertTrue(ex.getCause() instanceof NullPointerException);
        }

        // only one member
        var e2 = InternalFunctionService.onServers(c, "g1");
        args.add("g2");
        e2 = e2.setArguments(args);
        try {
          e2.execute(new OnGroupsExceptionFunction()).getResult();
          fail("expected exception not thrown");
        } catch (FunctionException ex) {
          assertTrue(ex.getCause() instanceof NullPointerException);
        }
        return null;
      }
    });
  }

  @Test
  public void testClientServerMemberFailure() {
    var host = Host.getHost(0);
    var server0 = host.getVM(0);
    var server1 = host.getVM(1);
    var server2 = host.getVM(2);
    var client = host.getVM(3);
    var locator = Host.getLocator();
    final var regionName = getName();

    initVM(server0, "mg,g0", regionName, true);
    initVM(server1, "g1", regionName, true);
    initVM(server2, "g0,g1,g2", regionName, true);

    final var locatorPort = getLocatorPort(locator);
    final var hostName = host.getHostName();

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          var c = CacheFactory.getAnyInstance();
          c.close();
        } catch (CacheClosedException ignored) {
        }
        disconnectFromDS();
        LogWriterUtils.getLogWriter().fine("SWAP:creating client cache");
        var ccf = new ClientCacheFactory();
        ccf.addPoolLocator(hostName, locatorPort);
        ccf.setPoolServerGroup("mg");
        ccf.set(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
        var c = ccf.create();

        var e = InternalFunctionService.onServers(c, "g1");
        var args = new ArrayList<String>();
        args.add("disconnect");
        e = e.setArguments(args);

        IgnoredException.addIgnoredException("FunctionInvocationTargetException");
        try {
          e.execute(new OnGroupsExceptionFunction()).getResult();
          fail("expected exception not thrown");
        } catch (FunctionException ex) {
          assertTrue(ex.getCause() instanceof FunctionInvocationTargetException);
        }
        return null;
      }
    });
  }

  @Test
  public void testClientServerOneMemberFailure() {
    var host = Host.getHost(0);
    var server0 = host.getVM(0);
    var server1 = host.getVM(1);
    var server2 = host.getVM(2);
    var client = host.getVM(3);
    var locator = Host.getLocator();
    final var regionName = getName();

    initVM(server0, "mg,g0", regionName, true);
    initVM(server1, "g1", regionName, true);
    initVM(server2, "g0,g1,g2", regionName, true);

    final var locatorPort = getLocatorPort(locator);
    final var hostName = host.getHostName();

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          var c = CacheFactory.getAnyInstance();
          c.close();
        } catch (CacheClosedException ignored) {
        }
        disconnectFromDS();
        LogWriterUtils.getLogWriter().fine("SWAP:creating client cache");
        var ccf = new ClientCacheFactory();
        ccf.addPoolLocator(hostName, locatorPort);
        ccf.setPoolServerGroup("mg");
        ccf.set(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
        var c = ccf.create();

        var e = InternalFunctionService.onServers(c, "g1");
        var args = new ArrayList<String>();
        args.add("disconnect");
        args.add("g2");
        e = e.setArguments(args);
        IgnoredException.addIgnoredException("FunctionInvocationTargetException");
        try {
          e.execute(new OnGroupsExceptionFunction()).getResult();
          fail("expected exception not thrown");
        } catch (FunctionException ex) {
          assertTrue(ex.getCause() instanceof FunctionInvocationTargetException);
        }
        return null;
      }
    });
  }

  @Test
  public void testClientServerIgnoreMemberFailure() {
    var host = Host.getHost(0);
    var server0 = host.getVM(0);
    var server1 = host.getVM(1);
    var server2 = host.getVM(2);
    var client = host.getVM(3);
    var locator = Host.getLocator();
    final var regionName = getName();

    initVM(server0, "mg,g0", regionName, true);
    initVM(server1, "g1", regionName, true);
    initVM(server2, "g0,g1,g2", regionName, true);

    final var locatorPort = getLocatorPort(locator);
    final var hostName = host.getHostName();

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          var c = CacheFactory.getAnyInstance();
          c.close();
        } catch (CacheClosedException ignored) {
        }
        disconnectFromDS();
        LogWriterUtils.getLogWriter().fine("SWAP:creating client cache");
        var ccf = new ClientCacheFactory();
        ccf.addPoolLocator(hostName, locatorPort);
        ccf.setPoolServerGroup("mg");
        ccf.set(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
        var c = ccf.create();

        var e = InternalFunctionService.onServers(c, "g1");
        var args = new ArrayList<String>();
        args.add("disconnect");
        args.add("g2");
        e = e.setArguments(args);
        ((AbstractExecution) e).setIgnoreDepartedMembers(true);
        var l = (ArrayList) e.execute(new OnGroupsExceptionFunction()).getResult();
        LogWriterUtils.getLogWriter().info("SWAP:result:" + l);
        assertEquals(2, l.size());
        if (l.get(0) instanceof Throwable) {
          assertTrue((Boolean) l.get(1));
        } else if (l.get(0) instanceof Boolean) {
          assertTrue(l.get(1) instanceof Throwable);
        } else {
          fail("expected to find a Boolean or throwable at index 0");
        }
        return null;
      }
    });
  }

  static class OnGroupsNoAckFunction extends OnGroupsFunction {

    public OnGroupsNoAckFunction() {}

    @Override
    public boolean hasResult() {
      return false;
    }

    @Override
    public boolean isHA() {
      return false;
    }
  }

  @Test
  public void testNoAckGroupsFunction() {
    // Workaround for #52005. This is a product bug
    // that should be fixed
    addIgnoredException("Cannot return any result");
    var host = getHost(0);
    final var server0 = host.getVM(0);
    final var server1 = host.getVM(1);
    final var server2 = host.getVM(2);
    var client = host.getVM(3);
    var locator = getLocator();
    final var regionName = getName();

    initVM(server0, "mg,g0", regionName, true);
    initVM(server1, "g1", regionName, true);
    initVM(server2, "g0,g1", regionName, true);

    final var locatorPort = getLocatorPort(locator);
    final var hostName = host.getHostName();

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          var c = getAnyInstance();
          c.close();
        } catch (CacheClosedException ignored) {
        }
        disconnectFromDS();
        getLogWriter().fine("SWAP:creating client cache");
        var ccf = new ClientCacheFactory();
        ccf.addPoolLocator(hostName, locatorPort);
        ccf.setPoolServerGroup("mg");
        ccf.set(LOG_LEVEL, getDUnitLogLevel());
        var c = ccf.create();

        c.getLogger().info("SWAP:invoking function from client on g0");
        var e = onServers(c, "g0");
        e.execute(new OnGroupsNoAckFunction());
        return null;
      }
    });
    var wc = new WaitCriterion() {
      @Override
      public boolean done() {
        var c0 = getInvocationCount(server0);
        var c1 = getInvocationCount(server1);
        var c2 = getInvocationCount(server2);
        return (c0 + c1 + c2) == 2;
      }

      @Override
      public String description() {
        return "OnGroupsNoAck invocation count mismatch";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);

    resetInvocationCount(server0);
    resetInvocationCount(server1);
    resetInvocationCount(server2);

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var c = ClientCacheFactory.getAnyInstance();
        var e = onServer(c, "g1");
        e.execute(new OnGroupsNoAckFunction());
        return null;
      }
    });
    // pause here to verify that we do not get more than 1 invocation
    pause(5000);
    var wc2 = new WaitCriterion() {
      @Override
      public boolean done() {
        var c0 = getInvocationCount(server0);
        var c1 = getInvocationCount(server1);
        var c2 = getInvocationCount(server2);
        return (c0 + c1 + c2) == 1;
      }

      @Override
      public String description() {
        return "OnGroupsNoAck invocation count mismatch";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc2);
    resetInvocationCount(server0);
    resetInvocationCount(server1);
    resetInvocationCount(server2);
  }
}
