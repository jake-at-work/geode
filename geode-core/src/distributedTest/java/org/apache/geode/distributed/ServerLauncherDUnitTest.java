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
import static org.apache.geode.test.process.JavaModuleHelper.getJvmModuleOptions;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Paths;

import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Cache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.membership.MembershipEvent;
import org.apache.geode.management.membership.MembershipListener;
import org.apache.geode.management.membership.UniversalMembershipListenerAdapter;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;


public class ServerLauncherDUnitTest {

  private static final Logger logger = LogService.getLogger();

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  public static class TestManagementListener extends UniversalMembershipListenerAdapter {

    public static boolean crashed = false;
    public static boolean joined = false;
    public static boolean left = false;

    @Override
    public void memberCrashed(MembershipEvent event) {
      crashed = true;
    }

    @Override
    public void memberJoined(MembershipEvent event) {
      joined = true;
    }

    @Override
    public void memberLeft(MembershipEvent event) {
      left = true;
    }
  }

  @Test
  public void ensureCleanShutdownFromInProcessServerLauncher() throws Exception {
    var locator = cluster.startLocatorVM(0);

    // Start a server who will be a lead and thus have a weight of 15. If we don't do this and the
    // test fails with just a single server crashing, the locator will declare a split-brain and
    // shut itself down.
    cluster.startServerVM(1, locator.getPort());

    locator.invoke(() -> {
      MembershipListener listener = new TestManagementListener();
      Cache cache = ClusterStartupRule.getCache();
      var managementService = ManagementService.getExistingManagementService(cache);
      managementService.addMembershipListener(listener);
    });

    launchServer(locator.getPort());

    await().until(
        () -> locator.invoke(() -> TestManagementListener.joined && TestManagementListener.left));

    assertThat(locator.invoke(() -> TestManagementListener.crashed)).isFalse();
  }

  private void launchServer(int port) throws Exception {
    var javaBin = Paths.get(System.getProperty("java.home"), "bin", "java");

    var serverLauncherClass = ServerLauncherDUnitTestHelper.class.getName();
    logger.info("Running java class " + serverLauncherClass);

    var pBuilder = new ProcessBuilder();
    pBuilder.directory(tempDir.newFolder());
    pBuilder.command(javaBin.toString(), "-classpath", System.getProperty("java.class.path"));
    // Copy all --add-opens and --add-exports options to the command line
    var command = pBuilder.command();
    command.addAll(getJvmModuleOptions());
    command.add(serverLauncherClass);
    command.add(String.valueOf(port));

    pBuilder.redirectErrorStream(true);
    var process = pBuilder.start();

    var result = new ByteArrayOutputStream();
    var bais = new BufferedInputStream(process.getInputStream());

    var buffer = new byte[4096];
    int n;
    while ((n = bais.read(buffer)) > 0) {
      result.write(buffer, 0, n);
    }

    if (process.waitFor() != 0) {
      logger.error(result.toString());
    }
  }
}
