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

package org.apache.geode.management.internal.configuration;

import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.classloader.ClassPathLoader;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.compiler.JarBuilder;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

public class ClusterConfigServerRestartWithJarDeployDUnitTest {

  @Rule
  public ClusterStartupRule rule = new ClusterStartupRule(5);

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Test
  public void functionExecutionAfterServerReconnect() throws Exception {
    IgnoredException.addIgnoredException("org.apache.geode.ForcedDisconnectException: for testing");
    IgnoredException.addIgnoredException("cluster configuration service not available");
    IgnoredException.addIgnoredException("This thread has been stalled");
    IgnoredException
        .addIgnoredException("member unexpectedly shut down shared, unordered connection");
    IgnoredException.addIgnoredException("Connection refused");

    var locator0 = rule.startLocatorVM(0);
    gfsh.connectAndVerify(locator0);

    gfsh.executeAndAssertThat(
        "configure pdx --read-serialized=true --auto-serializable-classes=ClusterConfigServerRestartWithJarDeployFunction.*");

    var props = new Properties();
    var server1 = rule.startServerVM(1, props, locator0.getPort());
    var server2 = rule.startServerVM(2, props, locator0.getPort());

    var functionJar = getFunctionJar();
    gfsh.executeAndAssertThat("deploy --jar=" + functionJar.getAbsolutePath()).statusIsSuccess();

    callFunction(server1);

    server2.forceDisconnect();

    server2.waitTilFullyReconnected();

    callFunction(server1);
  }

  private File getFunctionJar() throws IOException {
    var jarBuilder = new JarBuilder();
    var filePath =
        createTempFileFromResource(getClass(),
            "/ClusterConfigServerRestartWithJarDeployFunction.java").getAbsolutePath();
    assertThat(filePath).as("java file resource not found").isNotBlank();

    var functionJar = new File(temporaryFolder.newFolder(), "output.jar");
    jarBuilder.buildJar(functionJar, new File(filePath));

    return functionJar;
  }

  private void callFunction(MemberVM member) {
    member.invoke(() -> {
      while (true) {
        try {
          var others =
              ClusterStartupRule.getCache().getDistributionManager()
                  .getOtherNormalDistributionManagerIds();
          var otherMember = others.stream().findFirst().get();

          var studentClass = ClassPathLoader.getLatest()
              .forName("ClusterConfigServerRestartWithJarDeployFunction$Student");

          var student = studentClass.getConstructor().newInstance();

          var collector = FunctionService.onMember(otherMember)
              .setArguments(student)
              .execute("student-function");

          var results = (List<Object>) collector.getResult();
          break;
        } catch (FunctionException fex) {
          if (fex.getCause() instanceof FunctionInvocationTargetException) {
            LogService.getLogger().info("Sleeping for 500ms after recoverable exception {}",
                fex.getMessage());
            Thread.sleep(500);
          } else {
            fail("Exception received from function execution: %s", fex.getMessage());
            throw fex;
          }
        }
      }
    });
  }

}
