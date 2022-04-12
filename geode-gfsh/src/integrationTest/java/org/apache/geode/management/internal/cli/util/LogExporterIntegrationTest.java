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
package org.apache.geode.management.internal.cli.util;

import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_ARCHIVE_FILE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.internal.cache.execute.FunctionContextImpl;
import org.apache.geode.management.internal.cli.functions.ExportLogsFunction;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({GfshTest.class, LoggingTest.class})
public class LogExporterIntegrationTest {
  @Rule
  public ServerStarterRule server = new ServerStarterRule();

  private final LogFilter filter = new LogFilter(Level.INFO, null, null);

  public Path serverFilesDir;

  @Before
  public void createServerFilesDir() throws IOException {
    // Name the directory after this test instance and the Gradle test worker, to ensure that tests
    // running in parallel use different directories.
    var testRunnerID = System.getProperty("org.gradle.test.worker", "standalone");
    var testInstanceID = System.identityHashCode(this);
    var className = getClass().getSimpleName();
    var dirName = String.format("%s-%x-%s", className, testInstanceID, testRunnerID);
    serverFilesDir = Files.createDirectories(Paths.get(dirName)).normalize().toAbsolutePath();
  }

  @After
  public void deleteServerFilesDir() {
    FileUtils.deleteQuietly(serverFilesDir.toFile());
  }

  @Test
  public void serverStartedWithWrongSuffix() throws Exception {
    var logFileNameWithWrongSuffix = "test.txt";
    var statsFileNameWithWrongSuffix = "archive.archive";

    var logFile = serverFilesDir.resolve(logFileNameWithWrongSuffix);
    var statsFile = serverFilesDir.resolve(statsFileNameWithWrongSuffix);

    server.withProperty(LOG_FILE, logFile.toString())
        .withProperty(STATISTIC_ARCHIVE_FILE, statsFile.toString())
        .startServer();

    var logExporter = new LogExporter(filter, null, null);
    var logFiles = logExporter.findLogFiles(serverFilesDir);

    assertThat(logFiles)
        .as("log files")
        .isEmpty();

    var statsFiles = logExporter.findStatFiles(serverFilesDir);
    assertThat(statsFiles)
        .as("stat files")
        .isEmpty();
  }

  @Test
  public void serverStartedWithCorrectSuffix() throws Exception {
    var logFileName = "test.log";
    var statsFileName = "archive.gfs";
    var logFile = serverFilesDir.resolve(logFileName);
    var statsFile = serverFilesDir.resolve(statsFileName);

    server.withProperty(LOG_FILE, logFile.toString())
        .withProperty(STATISTIC_ARCHIVE_FILE, statsFile.toString())
        .startServer();

    var logExporter = new LogExporter(filter, null, null);
    var logFiles = logExporter.findLogFiles(serverFilesDir);

    assertThat(logFiles)
        .as("log files")
        .hasSize(1);
    assertThat(logFiles.get(0)).hasFileName(logFileName);

    var statsFiles = logExporter.findStatFiles(serverFilesDir);
    assertThat(statsFiles)
        .as("stat files")
        .hasSize(1);
    assertThat(statsFiles.get(0)).hasFileName(statsFileName);
  }

  @Test
  @Ignore("GEODE-2574: fix .gz suffix")
  public void serverStartedWithGZSuffix() throws Exception {
    var gzLogFile = serverFilesDir.resolve("test.log.gz");
    var gzStatsFile = serverFilesDir.resolve("archive.gfs.gz");

    server.withProperty(LOG_FILE, gzLogFile.toString())
        .withProperty(STATISTIC_ARCHIVE_FILE, gzStatsFile.toString())
        .startServer();

    var logExporter = new LogExporter(filter, null, null);
    var logFiles = logExporter.findLogFiles(serverFilesDir);

    assertThat(logFiles)
        .as("log files")
        .hasSize(1);

    var statsFiles = logExporter.findStatFiles(serverFilesDir);
    assertThat(statsFiles)
        .as("stats files")
        .hasSize(1);
  }

  @Test
  public void testNoStatsFile() throws Throwable {
    var logFile = serverFilesDir.resolve("server.log");

    server.withProperty(LOG_FILE, logFile.toString())
        .startServer();

    verifyExportLogsFunctionDoesNotBlowUp(server.getCache());
  }

  @Test
  public void testWithRelativeFilePaths() throws Throwable {
    var serverWorkingDir = server.getWorkingDir().toPath().normalize().toAbsolutePath();
    var relativeLogFile = serverWorkingDir.relativize(serverFilesDir.resolve("server.log"));
    var relativeStatsFile = serverWorkingDir.relativize(serverFilesDir.resolve("stats.gfs"));

    server.withProperty(LOG_FILE, relativeLogFile.toString())
        .withProperty(STATISTIC_ARCHIVE_FILE, relativeStatsFile.toString())
        .startServer();

    verifyExportLogsFunctionDoesNotBlowUp(server.getCache());
  }

  @Test
  public void testWithAbsoluteFilePaths() throws Exception {
    var logFileName = "server.log";
    var statsFileName = "stats.gfs";
    var absoluteLogFile = serverFilesDir.resolve("logs").resolve(logFileName).toAbsolutePath();
    var absoluteStatsFile =
        serverFilesDir.resolve("stats").resolve(statsFileName).toAbsolutePath();
    Files.createDirectories(absoluteLogFile.getParent());
    Files.createDirectories(absoluteLogFile.getParent());

    server.withProperty(LOG_FILE, absoluteLogFile.toString())
        .withProperty(STATISTIC_ARCHIVE_FILE, absoluteStatsFile.toString())
        .startServer();

    var logExporter =
        new LogExporter(filter, absoluteLogFile.toFile(), absoluteStatsFile.toFile());
    var exportedZip = logExporter.export().toString();

    assertThat(zipEntriesIn(exportedZip))
        .containsExactlyInAnyOrder(logFileName, statsFileName);
  }

  private static void verifyExportLogsFunctionDoesNotBlowUp(Cache cache) throws Throwable {
    var args =
        new ExportLogsFunction.Args(null, null, "info", false, false, false);
    var resultSender = new CapturingResultSender();
    @SuppressWarnings("unchecked")
    FunctionContext<ExportLogsFunction.Args> context =
        new FunctionContextImpl(cache, "functionId", args, resultSender);
    new ExportLogsFunction().execute(context);
    if (resultSender.getThrowable() != null) {
      throw resultSender.getThrowable();
    }
  }

  private static Set<String> zipEntriesIn(String zipFilePath) throws IOException {
    return new ZipFile(zipFilePath).stream().map(ZipEntry::getName).collect(Collectors.toSet());
  }

  private static class CapturingResultSender implements ResultSender<Object> {

    private Throwable throwable;

    public Throwable getThrowable() {
      return throwable;
    }

    @Override
    public void sendResult(Object oneResult) {
      // nothing
    }

    @Override
    public void lastResult(Object lastResult) {
      // nothing
    }

    @Override
    public void sendException(Throwable t) {
      throwable = t;
    }
  }
}
