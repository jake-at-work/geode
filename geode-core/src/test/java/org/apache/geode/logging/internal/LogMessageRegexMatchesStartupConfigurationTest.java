/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.logging.internal;

import static org.apache.geode.logging.internal.LogMessageRegex.Group.DATE;
import static org.apache.geode.logging.internal.LogMessageRegex.Group.LOG_LEVEL;
import static org.apache.geode.logging.internal.LogMessageRegex.Group.MEMBER_NAME;
import static org.apache.geode.logging.internal.LogMessageRegex.Group.MESSAGE;
import static org.apache.geode.logging.internal.LogMessageRegex.Group.THREAD_ID;
import static org.apache.geode.logging.internal.LogMessageRegex.Group.THREAD_NAME;
import static org.apache.geode.logging.internal.LogMessageRegex.Group.TIME;
import static org.apache.geode.logging.internal.LogMessageRegex.Group.TIME_ZONE;
import static org.apache.geode.logging.internal.LogMessageRegex.Group.values;
import static org.apache.geode.logging.internal.LogMessageRegex.getPattern;
import static org.apache.geode.logging.internal.LogMessageRegex.getRegex;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Unit tests for {@link LogMessageRegex} matching startup configuration with optional
 * {@code memberName} missing.
 *
 * <p>
 * Example log message containing startup configuration:
 *
 * <pre>
 * [info 2018/10/19 16:03:18.069 PDT <main> tid=0x1] Startup Configuration: ### GemFire Properties defined with api ###
 * </pre>
 */
@Category(LoggingTest.class)
public class LogMessageRegexMatchesStartupConfigurationTest {

  private final String logLevel = "info";
  private final String date = "2018/10/19";
  private final String time = "16:03:18.069";
  private final String timeZone = "PDT";
  private final String memberName = "";
  private final String threadName = "<main>";
  private final String threadId = "tid=0x1";
  private final String message =
      "Startup Configuration: ### GemFire Properties defined with api ###";

  private String logLine;

  @Before
  public void setUp() {
    logLine = "[" + logLevel + " " + date + " " + time + " " + timeZone + " " + memberName
        + threadName + " " + threadId + "] " + message;
  }

  @Test
  public void regexMatchesStartupConfigurationLogLine() {
    assertThat(logLine).matches(getRegex());
  }

  @Test
  public void patternMatcherMatchesStartupConfigurationLogLine() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void patternMatcherGroupZeroMatchesStartupConfigurationLogLine() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(0)).isEqualTo(logLine);
  }

  @Test
  public void patternMatcherGroupCountEqualsGroupsLength() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.groupCount()).isEqualTo(values().length);
  }

  @Test
  public void logLevelGroupIndexCapturesStartupConfigurationLogLevel() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(LOG_LEVEL.getIndex())).isEqualTo(logLevel);
  }

  @Test
  public void dateGroupIndexCapturesStartupConfigurationDate() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(DATE.getIndex())).isEqualTo(date);
  }

  @Test
  public void timeGroupIndexCapturesStartupConfigurationTime() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(TIME.getIndex())).isEqualTo(time);
  }

  @Test
  public void timeZoneGroupIndexCapturesStartupConfigurationTimeZone() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(TIME_ZONE.getIndex())).isEqualTo(timeZone);
  }

  @Test
  public void logLevelGroupNameCapturesStartupConfigurationLogLevel() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(LOG_LEVEL.getName())).isEqualTo(logLevel);
  }

  @Test
  public void dateGroupNameCapturesStartupConfigurationDate() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(DATE.getName())).isEqualTo(date);
  }

  @Test
  public void timeGroupNameCapturesStartupConfigurationTime() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(TIME.getName())).isEqualTo(time);
  }

  @Test
  public void timeZoneGroupNameCapturesStartupConfigurationTimeZone() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(TIME_ZONE.getName())).isEqualTo(timeZone);
  }

  @Test
  public void memberNameGroupNameCapturesStartupConfigurationMemberName() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(MEMBER_NAME.getName())).isEqualTo(memberName);
  }

  @Test
  public void threadNameGroupNameCapturesStartupConfigurationThreadName() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(THREAD_NAME.getName())).isEqualTo(threadName);
  }

  @Test
  public void threadIdGroupNameCapturesStartupConfigurationThreadId() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(THREAD_ID.getName())).isEqualTo(threadId);
  }

  @Test
  public void messageGroupNameCapturesStartupConfigurationMessage() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(MESSAGE.getName())).isEqualTo(message);
  }

  @Test
  public void logLevelRegexMatchesStartupConfigurationLogLevel() {
    var matcher = Pattern.compile(LOG_LEVEL.getRegex()).matcher(logLevel);
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void dateRegexMatchesStartupConfigurationDate() {
    var matcher = Pattern.compile(DATE.getRegex()).matcher(date);
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void timeRegexMatchesStartupConfigurationTime() {
    var matcher = Pattern.compile(TIME.getRegex()).matcher(time);
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void timeZoneRegexMatchesStartupConfigurationTimeZone() {
    var matcher = Pattern.compile(TIME_ZONE.getRegex()).matcher(timeZone);
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void memberNameRegexMatchesStartupConfigurationMemberName() {
    var matcher = Pattern.compile(MEMBER_NAME.getRegex()).matcher(memberName);
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void threadNameRegexMatchesStartupConfigurationThreadName() {
    var matcher = Pattern.compile(THREAD_NAME.getRegex()).matcher(threadName);
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void threadIdRegexMatchesStartupConfigurationThreadId() {
    var matcher = Pattern.compile(THREAD_ID.getRegex()).matcher(threadId);
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void messageRegexMatchesStartupConfigurationMessage() {
    var matcher = Pattern.compile(MESSAGE.getRegex()).matcher(message);
    assertThat(matcher.matches()).isTrue();
  }
}
