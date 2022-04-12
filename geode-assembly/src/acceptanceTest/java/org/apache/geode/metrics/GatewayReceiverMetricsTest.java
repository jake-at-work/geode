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

package org.apache.geode.metrics;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.compiler.ClassBuilder.writeJarFromClasses;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.rules.ServiceJarRule;
import org.apache.geode.test.junit.categories.MetricsTest;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

@Category(MetricsTest.class)
public class GatewayReceiverMetricsTest {

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ServiceJarRule serviceJarRule = new ServiceJarRule();

  private static final String SENDER_LOCATOR_NAME = "sender-locator";
  private static final String RECEIVER_LOCATOR_NAME = "receiver-locator";
  private static final String SENDER_SERVER_NAME = "sender-server";
  private static final String RECEIVER_SERVER_NAME = "receiver-server";
  private static final String REGION_NAME = "region";
  private static final String GFSH_COMMAND_SEPARATOR = " ";
  private String senderLocatorFolder;
  private String receiverLocatorFolder;
  private String senderServerFolder;
  private String receiverServerFolder;
  private int receiverLocatorPort;
  private int senderLocatorPort;

  @Before
  public void startClusters() throws IOException {
    var ports = AvailablePortHelper.getRandomAvailableTCPPorts(6);

    receiverLocatorPort = ports[0];
    senderLocatorPort = ports[1];
    var senderServerPort = ports[2];
    var receiverServerPort = ports[3];
    var senderLocatorJmxPort = ports[4];
    var receiverLocatorJmxPort = ports[5];

    var senderSystemId = 2;
    var receiverSystemId = 1;

    senderLocatorFolder = newFolder(SENDER_LOCATOR_NAME);
    receiverLocatorFolder = newFolder(RECEIVER_LOCATOR_NAME);
    senderServerFolder = newFolder(SENDER_SERVER_NAME);
    receiverServerFolder = newFolder(RECEIVER_SERVER_NAME);

    var startSenderLocatorCommand = String.join(GFSH_COMMAND_SEPARATOR,
        "start locator",
        "--name=" + SENDER_LOCATOR_NAME,
        "--dir=" + senderLocatorFolder,
        "--port=" + senderLocatorPort,
        "--locators=localhost[" + senderLocatorPort + "]",
        "--http-service-port=0",
        "--J=-Dgemfire.remote-locators=localhost[" + receiverLocatorPort + "]",
        "--J=-Dgemfire.distributed-system-id=" + senderSystemId,
        "--J=-Dgemfire.jmx-manager-start=true",
        "--J=-Dgemfire.jmx-manager-port=" + senderLocatorJmxPort);

    var startReceiverLocatorCommand = String.join(GFSH_COMMAND_SEPARATOR,
        "start locator",
        "--name=" + RECEIVER_LOCATOR_NAME,
        "--dir=" + receiverLocatorFolder,
        "--port=" + receiverLocatorPort,
        "--locators=localhost[" + receiverLocatorPort + "]",
        "--http-service-port=0",
        "--J=-Dgemfire.remote-locators=localhost[" + senderLocatorPort + "]",
        "--J=-Dgemfire.distributed-system-id=" + receiverSystemId,
        "--J=-Dgemfire.jmx-manager-start=true ",
        "--J=-Dgemfire.jmx-manager-port=" + receiverLocatorJmxPort);

    var startSenderServerCommand = String.join(GFSH_COMMAND_SEPARATOR,
        "start server",
        "--name=" + SENDER_SERVER_NAME,
        "--dir=" + senderServerFolder,
        "--locators=localhost[" + senderLocatorPort + "]",
        "--server-port=" + senderServerPort,
        "--J=-Dgemfire.distributed-system-id=" + senderSystemId);

    var serviceJarPath = serviceJarRule.createJarFor("metrics-publishing-service.jar",
        MetricsPublishingService.class, SimpleMetricsPublishingService.class);

    var startReceiverServerCommand = String.join(GFSH_COMMAND_SEPARATOR,
        "start server",
        "--name=" + RECEIVER_SERVER_NAME,
        "--dir=" + receiverServerFolder,
        "--locators=localhost[" + receiverLocatorPort + "]",
        "--server-port=" + receiverServerPort,
        "--classpath=" + serviceJarPath,
        "--J=-Dgemfire.distributed-system-id=" + receiverSystemId);

    gfshRule.execute(startSenderLocatorCommand, startReceiverLocatorCommand,
        startSenderServerCommand, startReceiverServerCommand);

    var gatewaySenderId = "gs";

    var connectToSenderLocatorCommand = "connect --locator=localhost[" + senderLocatorPort + "]";

    var startGatewaySenderCommand = String.join(GFSH_COMMAND_SEPARATOR,
        "create gateway-sender",
        "--id=" + gatewaySenderId,
        "--parallel=false",
        "--remote-distributed-system-id=" + receiverSystemId);

    var createSenderRegionCommand = String.join(GFSH_COMMAND_SEPARATOR,
        "create region",
        "--name=" + REGION_NAME,
        "--type=" + RegionShortcut.REPLICATE.name(),
        "--gateway-sender-id=" + gatewaySenderId);

    gfshRule.execute(connectToSenderLocatorCommand, startGatewaySenderCommand,
        createSenderRegionCommand);

    var connectToReceiverLocatorCommand =
        "connect --locator=localhost[" + receiverLocatorPort + "]";
    var startGatewayReceiverCommand = "create gateway-receiver";
    var createReceiverRegionCommand = String.join(GFSH_COMMAND_SEPARATOR,
        "create region",
        "--name=" + REGION_NAME,
        "--type=" + RegionShortcut.REPLICATE.name());

    gfshRule.execute(connectToReceiverLocatorCommand, startGatewayReceiverCommand,
        createReceiverRegionCommand);

    // Deploy function to members
    var functionJarPath =
        newJarForFunctionClass(GetEventsReceivedCountFunction.class, "function.jar");
    var deployCommand = "deploy --jar=" + functionJarPath;
    var listFunctionsCommand = "list functions";

    gfshRule.execute(connectToReceiverLocatorCommand, deployCommand, listFunctionsCommand);
  }

  @After
  public void stopClusters() {
    var stopReceiverServerCommand = "stop server --dir=" + receiverServerFolder;
    var stopSenderServerCommand = "stop server --dir=" + senderServerFolder;
    var stopReceiverLocatorCommand = "stop locator --dir=" + receiverLocatorFolder;
    var stopSenderLocatorCommand = "stop locator --dir=" + senderLocatorFolder;

    gfshRule.execute(stopReceiverServerCommand, stopSenderServerCommand, stopReceiverLocatorCommand,
        stopSenderLocatorCommand);
  }

  @Test
  public void whenPerformingOperations_thenGatewayReceiverEventsReceivedIncreases() {
    var connectToSenderLocatorCommand = "connect --locator=localhost[" + senderLocatorPort + "]";

    var doPutCommand = String.join(GFSH_COMMAND_SEPARATOR,
        "put",
        "--region=" + REGION_NAME,
        "--key=foo",
        "--value=bar");

    var doRemoveCommand = String.join(GFSH_COMMAND_SEPARATOR,
        "remove",
        "--region=" + REGION_NAME,
        "--key=foo");

    var doCreateRegionCommand = String.join(GFSH_COMMAND_SEPARATOR,
        "create region",
        "--name=blah",
        "--type=" + RegionShortcut.REPLICATE.name());

    gfshRule.execute(connectToSenderLocatorCommand, doPutCommand, doRemoveCommand,
        doCreateRegionCommand);

    var connectToReceiverLocatorCommand =
        "connect --locator=localhost[" + receiverLocatorPort + "]";
    var executeFunctionCommand = "execute function --id=" + GetEventsReceivedCountFunction.ID;

    Collection<String> gatewayEventsExpectedToReceive =
        Arrays.asList(doPutCommand, doRemoveCommand);

    await().untilAsserted(() -> {
      var output =
          gfshRule.execute(connectToReceiverLocatorCommand, executeFunctionCommand).getOutputText();

      assertThat(output.trim())
          .as("Returned count of events received.")
          .endsWith("[" + gatewayEventsExpectedToReceive.size() + ".0]");
    });
  }

  private String newFolder(String folderName) throws IOException {
    return temporaryFolder.newFolder(folderName).getAbsolutePath();
  }

  private String newJarForFunctionClass(Class clazz, String jarName) throws IOException {
    var jar = temporaryFolder.newFile(jarName);
    writeJarFromClasses(jar, clazz);
    return jar.getAbsolutePath();
  }

  public static class GetEventsReceivedCountFunction implements Function<Void> {
    static final String ID = "GetEventsReceivedCountFunction";

    @Override
    public void execute(FunctionContext<Void> context) {
      var eventsReceivedCounter = SimpleMetricsPublishingService.getRegistry()
          .find("geode.gateway.receiver.events")
          .counter();

      Object result = eventsReceivedCounter == null
          ? "Meter not found."
          : eventsReceivedCounter.count();

      context.getResultSender().lastResult(result);
    }

    @Override
    public String getId() {
      return ID;
    }
  }
}
