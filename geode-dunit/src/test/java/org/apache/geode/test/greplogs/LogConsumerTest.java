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
package org.apache.geode.test.greplogs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class LogConsumerTest {

  private static final String EXCEPTION_MESSAGE =
      "java.lang.ClassNotFoundException: does.not.Exist";

  private LogConsumer logConsumer;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {
    var allowSkipLogMessages = false;
    List<Pattern> expectedStrings = Collections.emptyList();
    var logFileName = getClass().getSimpleName() + "_" + testName.getMethodName();
    var repeatLimit = 2;

    logConsumer = new LogConsumer(allowSkipLogMessages, expectedStrings, logFileName, repeatLimit);
  }

  @Test
  public void consume_returnsNull_ifLineIsOk() {
    var value = logConsumer.consume("ok");

    assertThat(value).isNull();
  }

  @Test
  public void consume_returnsNull_ifLineIsEmpty() {
    var value = logConsumer.consume("");

    assertThat(value).isNull();
  }

  @Test
  public void consume_throwsNullPointerException_ifLineIsNull() {
    var thrown = catchThrowable(() -> logConsumer.consume(null));

    assertThat(thrown)
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void close_returnsNull_ifLineIsOk() {
    logConsumer.consume("ok");

    var value = logConsumer.close();

    assertThat(value).isNull();
  }

  @Test
  public void close_returnsNull_ifLineIsEmpty() {
    logConsumer.consume("");

    var value = logConsumer.close();

    assertThat(value).isNull();
  }

  @Test
  public void close_returnsNull_ifLineContains_infoLevelMessage_withException() {
    logConsumer.consume("[info 019/06/13 14:41:05.750 PDT <main> tid=0x1] " +
        NullPointerException.class.getName());

    var value = logConsumer.close();

    assertThat(value).isNull();
  }

  @Test
  public void close_returnsLine_ifLineContains_errorLevelMessage() {
    var line = "[error 019/06/13 14:41:05.750 PDT <main> tid=0x1] message";
    logConsumer.consume(line);

    var value = logConsumer.close();

    assertThat(value).contains(line);
  }

  @Test
  public void close_returnsNull_ifLineContains_warningLevelMessage() {
    logConsumer.consume("[warning 2019/06/13 14:41:05.750 PDT <main> tid=0x1] message");

    var value = logConsumer.close();

    assertThat(value).isNull();
  }

  @Test
  public void close_returnsLine_ifLineContains_fatalLevelMessage() {
    var line = "[fatal 2019/06/13 14:41:05.750 PDT <main> tid=0x1] message";
    logConsumer.consume(line);

    var value = logConsumer.close();

    assertThat(value).contains(line);
  }

  @Test
  public void close_returnsLine_ifLineContains_severeLevelMessage() {
    var line = "[severe 2019/06/13 14:41:05.750 PDT <main> tid=0x1] message";
    logConsumer.consume(line);

    var value = logConsumer.close();

    assertThat(value).contains(line);
  }

  @Test
  public void close_returnsLine_ifLineContains_malformedLog4jStatement() {
    var line = "[info 2019/06/13 14:41:05.750 PDT <main> tid=0x1] contains {}";
    logConsumer.consume(line);

    var value = logConsumer.close();

    assertThat(value).contains(line);
  }

  @Test
  public void close_returnsNull_ifLineContains_hydraMasterLocatorsWildcard() {
    var line = "hydra.MasterDescription.master.locators={}";
    logConsumer.consume(line);

    var value = logConsumer.close();

    assertThat(value).isNull();
  }

  @Test
  public void close_returnsLine_ifLineContainsException() {
    logConsumer.consume(EXCEPTION_MESSAGE);

    var value = logConsumer.close();

    assertThat(value).contains(EXCEPTION_MESSAGE);
  }
}
