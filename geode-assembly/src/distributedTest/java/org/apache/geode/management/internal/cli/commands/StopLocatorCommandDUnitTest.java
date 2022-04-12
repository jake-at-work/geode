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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.management.internal.i18n.CliStrings.GROUP;
import static org.apache.geode.management.internal.i18n.CliStrings.START_LOCATOR;
import static org.apache.geode.management.internal.i18n.CliStrings.START_LOCATOR__DIR;
import static org.apache.geode.management.internal.i18n.CliStrings.START_LOCATOR__J;
import static org.apache.geode.management.internal.i18n.CliStrings.START_LOCATOR__LOCATORS;
import static org.apache.geode.management.internal.i18n.CliStrings.START_LOCATOR__MEMBER_NAME;
import static org.apache.geode.management.internal.i18n.CliStrings.START_LOCATOR__PORT;
import static org.apache.geode.management.internal.i18n.CliStrings.STOP_LOCATOR;
import static org.apache.geode.management.internal.i18n.CliStrings.STOP_LOCATOR__DIR;
import static org.apache.geode.management.internal.i18n.CliStrings.STOP_LOCATOR__MEMBER;
import static org.apache.geode.management.internal.i18n.CliStrings.STOP_SERVICE__GFSH_NOT_CONNECTED_ERROR_MESSAGE;
import static org.apache.geode.util.internal.GeodeGlossary.GEMFIRE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.lang.ObjectUtils;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class StopLocatorCommandDUnitTest {
  private static MemberVM locator;
  private static final String memberName = "locatorToStop";
  private static final String groupName = "locatorGroup";
  private static String locatorConnectionString;
  private File workingDir;
  private static Integer jmxPort;

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @ClassRule
  public static final GfshCommandRule gfsh = new GfshCommandRule();

  @ClassRule
  public static final ClusterStartupRule cluster = new ClusterStartupRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    jmxPort = AvailablePortHelper.getRandomAvailableTCPPort();

    var props = new Properties();
    props.setProperty(ConfigurationProperties.JMX_MANAGER_PORT, jmxPort.toString());
    props.setProperty(ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS, "localhost");

    locator = cluster.startLocatorVM(0, props);

    locatorConnectionString = "localhost[" + locator.getPort() + "]";

    gfsh.connectAndVerify(locator);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("shutdown --include-locators").statusIsSuccess();
  }

  @Before
  public void before() throws Exception {
    workingDir = temporaryFolder.newFolder();
    var availablePorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    var locatorPort = availablePorts[0];
    var toStopJmxPort = availablePorts[1];

    final var command = new CommandStringBuilder(START_LOCATOR)
        .addOption(START_LOCATOR__MEMBER_NAME, memberName)
        .addOption(START_LOCATOR__LOCATORS, locatorConnectionString)
        .addOption(START_LOCATOR__PORT, Integer.toString(locatorPort))
        .addOption(GROUP, groupName)
        .addOption(START_LOCATOR__J,
            "-D" + GEMFIRE_PREFIX + "jmx-manager-port=" + toStopJmxPort)
        .addOption(START_LOCATOR__J,
            "-D" + GEMFIRE_PREFIX + "jmx-manager-hostname-for-clients=localhost")
        .addOption(START_LOCATOR__DIR, workingDir.getCanonicalPath())
        .getCommandString();

    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat(command).statusIsSuccess();
  }

  @After
  public void after() throws IOException {
    gfsh.executeAndAssertThat("stop locator --dir=" + workingDir.getCanonicalPath()).hasOutput();
  }

  @Test
  public void testWithDisconnectedGfsh() throws Exception {
    final var expectedError = CliStrings.format(STOP_SERVICE__GFSH_NOT_CONNECTED_ERROR_MESSAGE,
        CliStrings.LOCATOR_TERM_NAME);
    final var command = new CommandStringBuilder(STOP_LOCATOR)
        .addOption(STOP_LOCATOR__MEMBER, memberName)
        .getCommandString();

    if (gfsh.isConnected()) {
      gfsh.disconnect();
    }

    gfsh.executeAndAssertThat(command).statusIsError()
        .containsOutput(expectedError);

    gfsh.connectAndVerify(locator);
  }

  @Test
  public void testWithMemberName() {
    final var command = new CommandStringBuilder(STOP_LOCATOR)
        .addOption(STOP_LOCATOR__MEMBER, memberName)
        .getCommandString();


    // The new locator is not immediately available to be stopped because its mbean
    // has to be propagated to the existing locator that gfsh is connected to. Wait
    // for the stop to work
    waitForCommandToSucceed(command);
    gfsh.executeAndAssertThat("list members").doesNotContainOutput(memberName);
  }

  @Test
  public void testWithMemberID() {
    int port = jmxPort; // this assignment is needed to pass a local var into the invocation below

    final var memberID = locator.invoke(() -> getMemberId(port));

    final var command = new CommandStringBuilder(STOP_LOCATOR)
        .addOption(STOP_LOCATOR__MEMBER, memberID)
        .getCommandString();

    // The new locator is not immediately available to be stopped because its mbean
    // has to be propagated to the existing locator that gfsh is connected to. Wait
    // for the stop to work
    waitForCommandToSucceed(command);

    gfsh.executeAndAssertThat("list members").doesNotContainOutput(memberName);
  }

  private void waitForCommandToSucceed(String command) {
    GeodeAwaitility.await()
        .untilAsserted(() -> gfsh.executeAndAssertThat(command).statusIsSuccess());
  }

  @Test
  public void testWithDirOnline() throws IOException {
    final var command = new CommandStringBuilder(STOP_LOCATOR)
        .addOption(STOP_LOCATOR__DIR, workingDir.getCanonicalPath())
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess();
    gfsh.executeAndAssertThat("list members").doesNotContainOutput(memberName);
  }

  @Test
  public void testWithDirOffline() throws Exception {
    if (gfsh.isConnected()) {
      gfsh.disconnect();
    }

    final var command = new CommandStringBuilder(STOP_LOCATOR)
        .addOption(STOP_LOCATOR__DIR, workingDir.getCanonicalPath())
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess();

    gfsh.connect(locator);

    gfsh.executeAndAssertThat("list members").doesNotContainOutput(memberName);
  }

  @Test
  public void testWithInvalidMemberName() {
    final var command = new CommandStringBuilder(STOP_LOCATOR)
        .addOption(STOP_LOCATOR__MEMBER, "invalidMemberName")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsError();
    gfsh.executeAndAssertThat("list members").containsOutput(memberName);
  }

  @Test
  public void testWithInvalidMemberID() {
    final var command = new CommandStringBuilder(STOP_LOCATOR)
        .addOption(STOP_LOCATOR__MEMBER, "42")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsError();
    gfsh.executeAndAssertThat("list members").containsOutput(memberName);
  }

  protected static String getMemberId(int port) throws Exception {
    JMXConnector conn = null;

    try {
      final var objectNamePattern = ObjectName.getInstance("GemFire:type=Member,*");
      final var query = Query.eq(Query.attr("Name"), Query.value(memberName));
      final var url =
          new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:" + port + "/jmxrmi");
      System.out.println(url);

      assertThat(url).isNotNull();
      conn = JMXConnectorFactory.connect(url);
      assertThat(conn).isInstanceOf(JMXConnector.class);

      final var connection = conn.getMBeanServerConnection();
      assertThat(connection).isInstanceOf(MBeanServerConnection.class);

      GeodeAwaitility.await().untilAsserted(() -> {
        final var objectNames = connection.queryNames(objectNamePattern, query);
        assertThat(objectNames).isNotNull().isNotEmpty().hasSize(1);
      });

      final var objectNames = connection.queryNames(objectNamePattern, query);
      final var objectName = objectNames.iterator().next();
      final var memberId = connection.getAttribute(objectName, "Id");

      return ObjectUtils.toString(memberId);
    } finally {
      if (conn != null) {
        conn.close();
      }
    }
  }
}
