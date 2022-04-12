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

package org.apache.geode.management.internal.cli.shell;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.logging.LogManager;

import org.junit.Before;
import org.junit.Test;


public class GfshHeadlessModeUnitTest extends GfshAbstractUnitTest {

  @Override
  @Before
  public void before() {
    super.before();
    gfsh = new Gfsh(false, null, new GfshConfig());
  }

  @Test
  public void headlessModeShouldRedirectBothJDKAndGFSHLoggers() {
    gfsh = new Gfsh(false, null, new GfshConfig());
    var logManager = LogManager.getLogManager();
    var loggerNames = logManager.getLoggerNames();
    while (loggerNames.hasMoreElements()) {
      var loggerName = loggerNames.nextElement();
      var logger = logManager.getLogger(loggerName);
      // make sure jdk's logging goes to the gfsh log file
      if (loggerName.startsWith("java")) {
        assertThat(logger.getParent().getName()).endsWith("LogWrapper");
      }
      // make sure Gfsh's logging goes to the gfsh log file
      else if (loggerName.endsWith(".Gfsh")) {
        assertThat(logger.getParent().getName()).endsWith("LogWrapper");
      }
      // make sure SimpleParser's logging will still show up in the console
      else if (loggerName.endsWith(".SimpleParser")) {
        assertThat(logger.getParent().getName()).doesNotEndWith("LogWrapper");
      }
    }
  }
}
