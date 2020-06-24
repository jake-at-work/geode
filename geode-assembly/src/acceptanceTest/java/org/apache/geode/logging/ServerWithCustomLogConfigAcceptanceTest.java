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
package org.apache.geode.logging;

import static java.nio.file.Files.copy;
import static org.apache.geode.test.assertj.LogFileAssert.assertThat;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;

import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.test.junit.rules.RequiresGeodeHome;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

public class ServerWithCustomLogConfigAcceptanceTest {

  private static final String CONFIG_WITH_GEODE_PLUGINS_FILE_NAME =
      "ServerWithCustomLogConfigAcceptanceTestWithGeodePlugins.xml";
  private static final String CONFIG_WITHOUT_GEODE_PLUGINS_FILE_NAME =
      "ServerWithCustomLogConfigAcceptanceTestWithoutGeodePlugins.xml";

  private String serverName;
  private Path workingDir;
  private Path configWithGeodePluginsFile;
  private Path configWithoutGeodePluginsFile;
  private Path serverLogFile;
  private Path customLogFile;

  @Rule
  public GfshRule gfshRule = new GfshRule();
  @Rule
  public RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUpLogConfigFiles() {
    configWithGeodePluginsFile = createFileFromResource(
        getResource(CONFIG_WITH_GEODE_PLUGINS_FILE_NAME), temporaryFolder.getRoot(),
        CONFIG_WITH_GEODE_PLUGINS_FILE_NAME)
            .toPath();

    configWithoutGeodePluginsFile = createFileFromResource(
        getResource(CONFIG_WITHOUT_GEODE_PLUGINS_FILE_NAME), temporaryFolder.getRoot(),
        CONFIG_WITHOUT_GEODE_PLUGINS_FILE_NAME)
            .toPath();
  }

  @Before
  public void setUpOutputFiles() {
    serverName = testName.getMethodName();

    workingDir = temporaryFolder.getRoot().toPath().toAbsolutePath();
    serverLogFile = workingDir.resolve(serverName + ".log");
    customLogFile = workingDir.resolve("custom.log");
  }

  @After
  public void stopServer() {
    gfshRule.execute("stop server --dir=" + workingDir);
  }

  @Test
  public void serverLauncherUsesDefaultLoggingConfig() {
    String startLocatorCommand = String.join(" ",
        "start server",
        "--name=" + serverName,
        "--dir=" + workingDir,
        "--disable-default-server",
        "--locators=\"\"");

    gfshRule.execute(startLocatorCommand);

    await().untilAsserted(() -> {
      assertThat(serverLogFile.toFile())
          .as(serverLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("Server " + serverName + " startup completed in ")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class");

      assertThat(customLogFile.toFile())
          .as(customLogFile.toFile().getAbsolutePath())
          .doesNotExist();
    });
  }

  @Test
  public void serverLauncherUsesSpecifiedConfigFileWithoutGeodePlugins() {
    String startLocatorCommand = String.join(" ",
        "start server",
        "--name=" + serverName,
        "--dir=" + workingDir,
        "--disable-default-server",
        "--locators=\"\"",
        "--J=-Dlog4j.configurationFile=" + configWithoutGeodePluginsFile.toAbsolutePath());

    gfshRule.execute(startLocatorCommand);

    await().untilAsserted(() -> {
      assertThat(serverLogFile.toFile())
          .as(serverLogFile.toFile().getAbsolutePath())
          .doesNotExist();

      assertThat(customLogFile.toFile())
          .as(customLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("Server " + serverName + " startup completed in ")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class");
    });
  }

  @Test
  public void serverLauncherUsesConfigFileInClasspathWithoutGeodePlugins() throws Exception {
    copy(configWithoutGeodePluginsFile, workingDir.resolve("log4j2.xml"));

    String classpath = workingDir.toFile().getAbsolutePath();

    String startLocatorCommand = String.join(" ",
        "start server",
        "--name=" + serverName,
        "--dir=" + workingDir,
        "--disable-default-server",
        "--locators=\"\"",
        "--classpath", classpath);

    gfshRule.execute(startLocatorCommand);

    await().untilAsserted(() -> {
      assertThat(serverLogFile.toFile())
          .as(serverLogFile.toFile().getAbsolutePath())
          .doesNotExist();

      assertThat(customLogFile.toFile())
          .as(customLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("Server " + serverName + " startup completed in ")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class");
    });
  }

  @Test
  public void serverLauncherUsesSpecifiedConfigFileWithGeodePlugins() {
    String startLocatorCommand = String.join(" ",
        "start server",
        "--name=" + serverName,
        "--dir=" + workingDir,
        "--disable-default-server",
        "--locators=\"\"",
        "--J=-Dlog4j.configurationFile=" + configWithGeodePluginsFile.toAbsolutePath());

    gfshRule.execute(startLocatorCommand);

    await().untilAsserted(() -> {
      assertThat(serverLogFile.toFile())
          .as(serverLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("Server " + serverName + " startup completed in ")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class");

      assertThat(customLogFile.toFile())
          .as(customLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("Server " + serverName + " startup completed in ")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class");
    });
  }

  @Test
  public void serverLauncherUsesConfigFileInClasspathWithGeodePlugins() throws Exception {
    copy(configWithGeodePluginsFile, temporaryFolder.getRoot().toPath().resolve("log4j2.xml"));

    String classpath = workingDir.toFile().getAbsolutePath();

    String startLocatorCommand = String.join(" ",
        "start server",
        "--name=" + serverName,
        "--dir=" + workingDir,
        "--disable-default-server",
        "--locators=\"\"",
        "--classpath", classpath);

    gfshRule.execute(startLocatorCommand);

    await().untilAsserted(() -> {
      assertThat(serverLogFile.toFile())
          .as(serverLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("Server " + serverName + " startup completed in ")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class");

      assertThat(customLogFile.toFile())
          .as(customLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("Server " + serverName + " startup completed in ")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class");
    });
  }
}