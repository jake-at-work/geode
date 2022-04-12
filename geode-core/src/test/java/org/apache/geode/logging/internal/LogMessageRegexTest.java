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
 * Unit tests for {@link LogMessageRegex}.
 */
@Category(LoggingTest.class)
public class LogMessageRegexTest {

  private final String logLevel = "info";
  private final String date = "2018/09/24";
  private final String time = "12:35:59.515";
  private final String timeZone = "PDT";
  private final String memberName = "logMessageRegexTest";
  private final String threadName = "<main>";
  private final String threadId = "tid=0x1";
  private final String message = "this is a log statement";

  private String logLine;

  @Before
  public void setUp() {
    logLine = "[" + logLevel + " " + date + " " + time + " " + timeZone + " " + memberName + " "
        + threadName + " " + threadId + "] " + message;
  }

  @Test
  public void regexMatchesLogLine() {
    assertThat(logLine).matches(getRegex());
  }

  @Test
  public void patternMatcherMatchesLogLine() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void patternMatcherGroupZeroMatchesLogLine() {
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
  public void logLevelGroupIndexCapturesLogLevel() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(LOG_LEVEL.getIndex())).isEqualTo(logLevel);
  }

  @Test
  public void dateGroupIndexCapturesDate() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(DATE.getIndex())).isEqualTo(date);
  }

  @Test
  public void timeGroupIndexCapturesTime() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(TIME.getIndex())).isEqualTo(time);
  }

  @Test
  public void timeZoneGroupIndexCapturesTimeZone() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(TIME_ZONE.getIndex())).isEqualTo(timeZone);
  }

  @Test
  public void logLevelGroupNameCapturesLogLevel() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(LOG_LEVEL.getName())).isEqualTo(logLevel);
  }

  @Test
  public void dateGroupNameCapturesDate() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(DATE.getName())).isEqualTo(date);
  }

  @Test
  public void timeGroupNameCapturesTime() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(TIME.getName())).isEqualTo(time);
  }

  @Test
  public void timeZoneGroupNameCapturesTimeZone() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(TIME_ZONE.getName())).isEqualTo(timeZone);
  }

  @Test
  public void memberNameGroupNameCapturesMemberName() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(MEMBER_NAME.getName())).isEqualTo(memberName);
  }

  @Test
  public void threadNameGroupNameCapturesThreadName() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(THREAD_NAME.getName())).isEqualTo(threadName);
  }

  @Test
  public void threadIdGroupNameCapturesThreadId() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(THREAD_ID.getName())).isEqualTo(threadId);
  }

  @Test
  public void messageGroupNameCapturesMessage() {
    var matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(MESSAGE.getName())).isEqualTo(message);
  }

  @Test
  public void logLevelRegexMatchesLogLevel() {
    var matcher = Pattern.compile(LOG_LEVEL.getRegex()).matcher(logLevel);
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void dateRegexMatchesDate() {
    var matcher = Pattern.compile(DATE.getRegex()).matcher(date);
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void timeRegexMatchesTime() {
    var matcher = Pattern.compile(TIME.getRegex()).matcher(time);
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void timeZoneRegexMatchesTimeZone() {
    var matcher = Pattern.compile(TIME_ZONE.getRegex()).matcher(timeZone);
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void memberNameRegexMatchesMemberName() {
    var matcher = Pattern.compile(MEMBER_NAME.getRegex()).matcher(memberName);
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void threadNameRegexMatchesThreadName() {
    var matcher = Pattern.compile(THREAD_NAME.getRegex()).matcher(threadName);
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void threadIdRegexMatchesThreadId() {
    var matcher = Pattern.compile(THREAD_ID.getRegex()).matcher(threadId);
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void messageRegexMatchesMessage() {
    var matcher = Pattern.compile(MESSAGE.getRegex()).matcher(message);
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void memberNameRegexMatchesMissingMemberName() {
    var matcher = Pattern.compile(MEMBER_NAME.getRegex()).matcher("");
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void memberNameRegexMatchesMemberNameWithNoSpaces() {
    var matcher = Pattern.compile(MEMBER_NAME.getRegex()).matcher("");
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void memberNameRegexMatchesMemberNameWithOneSpace() {
    var matcher = Pattern.compile(MEMBER_NAME.getRegex()).matcher("hello world");
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void memberNameRegexMatchesMemberNameWithMultipleSpaces() {
    var matcher = Pattern.compile(MEMBER_NAME.getRegex()).matcher("this is a name");
    assertThat(matcher.matches()).isTrue();
  }
}
