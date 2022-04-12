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

package org.apache.geode.launchers;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.management.JMX;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.distributed.ServerLauncherCacheProvider;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.launchers.startuptasks.WaitForFileToExist;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.rules.ServiceJarRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

public class ServerStartupOnlineTest {

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Rule
  public ServiceJarRule serviceJarRule = new ServiceJarRule();

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  private Path serverFolder;
  private String serverName;
  private String startServerCommand;
  private int jmxRmiPort;

  @Before
  public void setup() {
    var serviceJarPath = serviceJarRule.createJarFor("ServerLauncherCacheProvider.jar",
        ServerLauncherCacheProvider.class, WaitForFileToExist.class);

    serverFolder = temporaryFolder.getRoot().toPath().toAbsolutePath();
    serverName = testName.getMethodName();

    var ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);

    var jmxHttpPort = ports[0];
    jmxRmiPort = ports[1];

    startServerCommand = String.join(" ",
        "start server",
        "--name=" + serverName,
        "--dir=" + serverFolder,
        "--classpath=" + serviceJarPath,
        "--disable-default-server",
        "--J=-Dgemfire.jmx-manager=true",
        "--J=-Dgemfire.jmx-manager-start=true",
        "--J=-Dgemfire.jmx-manager-http-port=" + jmxHttpPort,
        "--J=-Dgemfire.jmx-manager-port=" + jmxRmiPort);
  }

  @After
  public void stopServer() {
    var stopServerCommand = "stop server --dir=" + serverFolder;
    gfshRule.execute(stopServerCommand);
  }

  @Test
  public void startServerReturnsAfterStartupTaskCompletes() throws Exception {
    var startServerTask =
        executorServiceRule.runAsync(() -> gfshRule.execute(startServerCommand));

    waitForStartServerCommandToHang();

    assertThat(startServerTask).isNotDone();

    completeRemoteStartupTask();

    await().untilAsserted(() -> assertThat(startServerTask).isDone());
  }

  @Test
  public void statusServerReportsStartingUntilStartupTaskCompletes() throws Exception {
    var startServerTask =
        executorServiceRule.runAsync(() -> gfshRule.execute(startServerCommand));

    waitForStartServerCommandToHang();

    await().untilAsserted(() -> {
      var startingStatus = getServerStatusFromGfsh();
      assertThat(startingStatus)
          .as("Status server command output")
          .contains("Starting Server");
    });

    completeRemoteStartupTask();

    await().untilAsserted(() -> {
      assertThat(startServerTask).isDone();
      var onlineStatus = getServerStatusFromGfsh();
      assertThat(onlineStatus)
          .as("Status server command output")
          .contains("is currently online");
    });
  }

  @Test
  public void memberMXBeanStatusReportsStartingUntilStartupTaskCompletes() throws Exception {
    var startServerTask =
        executorServiceRule.runAsync(() -> gfshRule.execute(startServerCommand));

    waitForStartServerCommandToHang();

    await().ignoreExceptions().untilAsserted(() -> {
      // Get memberMXBean status
      var startingStatus = getServerStatusFromJmx();
      assertThat(startingStatus)
          .as("MemberMXBean status while starting")
          .isEqualTo("starting");
    });

    completeRemoteStartupTask();

    await().ignoreExceptions().untilAsserted(() -> {
      assertThat(startServerTask).isDone();
      var onlineStatus = getServerStatusFromJmx();
      assertThat(onlineStatus)
          .as("MemberMXBean status while online")
          .isEqualTo("online");
    });
  }

  private String getServerStatusFromJmx() throws MalformedObjectNameException,
      IOException {
    var objectName = ObjectName.getInstance("GemFire:type=Member,member=" + serverName);
    var url =
        new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:" + jmxRmiPort + "/jmxrmi");
    var jmxConnector = JMXConnectorFactory.connect(url, null);
    try {
      var mbeanServer = jmxConnector.getMBeanServerConnection();
      var memberMXBean =
          JMX.newMXBeanProxy(mbeanServer, objectName, MemberMXBean.class, false);
      var json = memberMXBean.status();
      return parseStatusFromJson(json);
    } finally {
      jmxConnector.close();
    }
  }

  private String parseStatusFromJson(String json) throws IOException {
    var mapper = new ObjectMapper();
    var jsonNode = mapper.readTree(json);
    return jsonNode.get("status").textValue();
  }

  private String getServerStatusFromGfsh() {
    var statusServerCommand = "status server --dir=" + serverFolder;
    return gfshRule.execute(statusServerCommand).getOutputText();
  }

  private void waitForStartServerCommandToHang()
      throws InterruptedException {
    await().untilAsserted(() -> assertThat(serverFolder.resolve(serverName + ".log")).exists());
    // Without sleeping, this test can pass when it shouldn't.
    Thread.sleep(10_000);
  }

  private void completeRemoteStartupTask() throws IOException {
    Files.createFile(serverFolder.resolve(WaitForFileToExist.WAITING_FILE_NAME));
  }
}
