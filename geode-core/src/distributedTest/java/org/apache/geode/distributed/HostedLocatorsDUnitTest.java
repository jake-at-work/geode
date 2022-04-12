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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertFalse;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Host.getHost;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.shell.support.util.FileUtils;

import org.apache.geode.distributed.AbstractLauncher.Status;
import org.apache.geode.distributed.LocatorLauncher.Builder;
import org.apache.geode.distributed.LocatorLauncher.LocatorState;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.util.StopWatch;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * Extracted from LocatorLauncherLocalIntegrationTest.
 *
 * @since GemFire 8.0
 */
@Category({ClientServerTest.class})
public class HostedLocatorsDUnitTest extends JUnit4DistributedTestCase {

  protected static final int TIMEOUT_MILLISECONDS = 5 * 60 * 1000; // 5 minutes

  protected transient volatile int locatorPort;
  protected transient volatile LocatorLauncher launcher;

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();
  }

  @Override
  public final void preTearDown() throws Exception {
    disconnectAllFromDS();
  }

  private String getUniqueLocatorName() {
    var uniqueLocatorName = Host.getHost(0).getHostName() + "_"
        + getUniqueName();
    return uniqueLocatorName;
  }

  @Test
  public void testGetAllHostedLocators() throws Exception {
    final var system = getSystem();
    final var dunitLocator = system.getConfig().getLocators();
    assertNotNull(dunitLocator);
    assertFalse(dunitLocator.isEmpty());

    final var ports = getRandomAvailableTCPPorts(4);

    final var uniqueName = getUniqueLocatorName();
    for (var i = 0; i < 4; i++) {
      final var whichvm = i;
      getHost(0).getVM(whichvm).invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          final var name = uniqueName + "-" + whichvm;
          final var subdir = new File(name);
          if (subdir.exists()) {
            FileUtils.deleteRecursively(subdir);
          }
          subdir.mkdir();
          assertTrue(subdir.exists() && subdir.isDirectory());

          final var builder = new Builder().setMemberName(name).setPort(ports[whichvm])
              .set(LOCATORS, dunitLocator)
              .setRedirectOutput(true).setWorkingDirectory(name);

          launcher = builder.build();
          assertEquals(Status.ONLINE, launcher.start().getStatus());
          waitForLocatorToStart(launcher, TIMEOUT_MILLISECONDS, 10, true);
          return null;
        }
      });
    }

    final var host = LocalHostUtil.getLocalHost().getHostAddress();

    final Set<String> locators = new HashSet<>();
    locators.add(host + "["
        + dunitLocator.substring(dunitLocator.indexOf("[") + 1, dunitLocator.indexOf("]")) + "]");
    for (var port : ports) {
      locators.add(host + "[" + port + "]");
    }

    // validation within non-locator
    final var dm =
        (ClusterDistributionManager) system.getDistributionManager();

    final var locatorIds = dm.getLocatorDistributionManagerIds();
    assertEquals(5, locatorIds.size());

    final var hostedLocators =
        dm.getAllHostedLocators();
    assertTrue(!hostedLocators.isEmpty());
    assertEquals(5, hostedLocators.size());

    for (var member : hostedLocators.keySet()) {
      assertEquals(1, hostedLocators.get(member).size());
      final var hostedLocator = hostedLocators.get(member).iterator().next();
      assertTrue(locators + " does not contain " + hostedLocator, locators.contains(hostedLocator));
    }

    // validate fix for #46324
    for (var whichvm = 0; whichvm < 4; whichvm++) {
      getHost(0).getVM(whichvm).invoke(new SerializableRunnable() {
        @Override
        public void run() {
          final var dm =
              (ClusterDistributionManager) InternalDistributedSystem.getAnyInstance()
                  .getDistributionManager();
          final var self = dm.getDistributionManagerId();

          final var locatorIds = dm.getLocatorDistributionManagerIds();
          assertTrue(locatorIds.contains(self));

          final var hostedLocators =
              dm.getAllHostedLocators();
          assertTrue(
              "hit bug #46324: " + hostedLocators + " is missing "
                  + InternalLocator.getLocatorStrings() + " for " + self,
              hostedLocators.containsKey(self));
        }
      });
    }

    // validation with locators
    for (var whichvm = 0; whichvm < 4; whichvm++) {
      getHost(0).getVM(whichvm).invoke(new SerializableRunnable() {
        @Override
        public void run() {
          final var dm =
              (ClusterDistributionManager) InternalDistributedSystem.getAnyInstance()
                  .getDistributionManager();

          final var locatorIds = dm.getLocatorDistributionManagerIds();
          assertEquals(5, locatorIds.size());

          final var hostedLocators =
              dm.getAllHostedLocators();
          assertTrue(!hostedLocators.isEmpty());
          assertEquals(5, hostedLocators.size());

          for (var member : hostedLocators.keySet()) {
            assertEquals(1, hostedLocators.get(member).size());
            final var hostedLocator = hostedLocators.get(member).iterator().next();
            assertTrue(locators + " does not contain " + hostedLocator,
                locators.contains(hostedLocator));
          }
        }
      });
    }
  }

  @Test
  public void testGetAllHostedLocatorsUsingPortZero() throws Exception {
    final var system = getSystem();
    final var dunitLocator = system.getConfig().getLocators();
    assertNotNull(dunitLocator);
    assertFalse(dunitLocator.isEmpty());

    // This will eventually contain the ports used by locators
    final var ports = new int[] {0, 0, 0, 0};

    final var uniqueName = getUniqueLocatorName();
    for (var i = 0; i < 4; i++) {
      final var whichvm = i;
      var port = (Integer) Host.getHost(0).getVM(whichvm).invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          final var name = uniqueName + "-" + whichvm;
          final var subdir = new File(name);
          if (subdir.exists()) {
            FileUtils.deleteRecursively(subdir);
          }
          subdir.mkdir();
          assertTrue(subdir.exists() && subdir.isDirectory());

          final var builder = new Builder().setMemberName(name).setPort(ports[whichvm])
              .set(LOCATORS, dunitLocator)
              .setRedirectOutput(true).setWorkingDirectory(name);

          launcher = builder.build();
          assertEquals(Status.ONLINE, launcher.start().getStatus());
          waitForLocatorToStart(launcher, TIMEOUT_MILLISECONDS, 10, true);
          return launcher.getPort();
        }
      });
      ports[i] = port;
    }

    final var host = LocalHostUtil.getLocalHost().getHostAddress();

    final Set<String> locators = new HashSet<>();
    locators.add(host + "["
        + dunitLocator.substring(dunitLocator.indexOf("[") + 1, dunitLocator.indexOf("]")) + "]");
    for (var port : ports) {
      locators.add(host + "[" + port + "]");
    }

    // validation within non-locator
    final var dm =
        (ClusterDistributionManager) system.getDistributionManager();

    final var locatorIds = dm.getLocatorDistributionManagerIds();
    assertEquals(5, locatorIds.size());

    final var hostedLocators =
        dm.getAllHostedLocators();
    assertTrue(!hostedLocators.isEmpty());
    assertEquals(5, hostedLocators.size());

    for (var member : hostedLocators.keySet()) {
      assertEquals(1, hostedLocators.get(member).size());
      final var hostedLocator = hostedLocators.get(member).iterator().next();
      assertTrue(locators + " does not contain " + hostedLocator, locators.contains(hostedLocator));
    }

    // validate fix for #46324
    for (var whichvm = 0; whichvm < 4; whichvm++) {
      Host.getHost(0).getVM(whichvm).invoke(new SerializableRunnable() {
        @Override
        public void run() {
          final var dm =
              (ClusterDistributionManager) InternalDistributedSystem.getAnyInstance()
                  .getDistributionManager();
          final var self = dm.getDistributionManagerId();

          final var locatorIds = dm.getLocatorDistributionManagerIds();
          assertTrue(locatorIds.contains(self));

          final var hostedLocators =
              dm.getAllHostedLocators();
          assertTrue(
              "hit bug #46324: " + hostedLocators + " is missing "
                  + InternalLocator.getLocatorStrings() + " for " + self,
              hostedLocators.containsKey(self));
        }
      });
    }

    // validation with locators
    for (var whichvm = 0; whichvm < 4; whichvm++) {
      Host.getHost(0).getVM(whichvm).invoke(new SerializableRunnable() {
        @Override
        public void run() {
          final var dm =
              (ClusterDistributionManager) InternalDistributedSystem.getAnyInstance()
                  .getDistributionManager();

          final var locatorIds = dm.getLocatorDistributionManagerIds();
          assertEquals(5, locatorIds.size());

          final var hostedLocators =
              dm.getAllHostedLocators();
          assertTrue(!hostedLocators.isEmpty());
          assertEquals(5, hostedLocators.size());

          for (var member : hostedLocators.keySet()) {
            assertEquals(1, hostedLocators.get(member).size());
            final var hostedLocator = hostedLocators.get(member).iterator().next();
            assertTrue(locators + " does not contain " + hostedLocator,
                locators.contains(hostedLocator));
          }
        }
      });
    }
  }

  protected void waitForLocatorToStart(final LocatorLauncher launcher, int timeout, int interval,
      boolean throwOnTimeout) throws Exception {
    assertEventuallyTrue("waiting for process to start: " + launcher.status(),
        () -> {
          try {
            final var LocatorState = launcher.status();
            return (LocatorState != null && Status.ONLINE.equals(LocatorState.getStatus()));
          } catch (RuntimeException e) {
            return false;
          }
        }, timeout, interval);
  }

  protected static void assertEventuallyTrue(final String message, final Callable<Boolean> callable,
      final int timeout, final int interval) throws Exception {
    var done = false;
    for (var time = new StopWatch(true); !done && time.elapsedTimeMillis() < timeout; done =
        (callable.call())) {
      Thread.sleep(interval);
    }
    assertTrue(message, done);
  }
}
