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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;


public class HyphenFormatterTest {

  private HyphenFormatter formatter;

  @Before
  public void setUp() {
    formatter = new HyphenFormatter();
  }

  @Test
  public void containsOptionWithOneOptionReturnsTrue() {
    var cmd = "start locator --name=loc1";
    assertTrue(formatter.containsOption(cmd));
  }

  @Test
  public void containsOptionWithNoOptionReturnsFalse() {
    var cmd = "start locator";
    assertFalse(formatter.containsOption(cmd));
  }

  @Test
  public void containsOptionWithMultipleOptionsReturnsTrue() {
    var cmd = "start locator --name=loc1 --J=-Dfoo=bar --J=-Dbar=foo";
    assertTrue(formatter.containsOption(cmd));
  }

  @Test
  public void valueWithoutQuotesReturnsWithQuotes() {
    var cmd = "start locator --name=loc1 --J=-Dfoo=bar";
    var formattedCmd = formatter.formatCommand(cmd);

    var expected = "start locator --name=loc1 --J=\"-Dfoo=bar\"";
    assertThat(formattedCmd).isEqualTo(expected);
  }

  @Test
  public void valueWithoutQuotesReturnsWithQuotes_2() {
    var cmd = "start locator --J=-Dfoo=bar --name=loc1";
    var formattedCmd = formatter.formatCommand(cmd);

    var expected = "start locator --J=\"-Dfoo=bar\" --name=loc1";
    assertThat(formattedCmd).isEqualTo(expected);
  }

  @Test
  public void valueWithHyphenWithoutQuotesFails() {
    var cmd =
        "rebalance --exclude-region=" + SEPARATOR
            + "GemfireDataCommandsDUnitTestRegion2 --simulate=true --time-out=-1";
    var formattedCmd = formatter.formatCommand(cmd);

    var expected =
        "rebalance --exclude-region=" + SEPARATOR
            + "GemfireDataCommandsDUnitTestRegion2 --simulate=true --time-out=\"-1\"";
    assertThat(formattedCmd).isEqualTo(expected);
  }

  @Test
  public void valueWithHyphenWithoutQuotes() {
    var cmd =
        "rebalance --exclude-region=" + SEPARATOR
            + "GemfireDataCommandsDUnitTestRegion2 --simulate=true --time-out=-1";
    var formattedCmd = formatter.formatCommand(cmd);

    var expected =
        "rebalance --exclude-region=" + SEPARATOR
            + "GemfireDataCommandsDUnitTestRegion2 --simulate=true --time-out=\"-1\"";
    assertThat(formattedCmd).isEqualTo(expected);
  }

  @Test
  public void nullShouldThrowNullPointerException() {
    assertThatThrownBy(() -> formatter.formatCommand(null))
        .isExactlyInstanceOf(NullPointerException.class);
  }

  @Test
  public void emptyShouldThrowNullPointerException() {
    assertThat(formatter.formatCommand("")).isEqualTo("");
  }

  @Test
  public void multipleJOptions() {
    var cmd = "start locator --name=loc1 --J=-Dfoo=bar --J=-Dbar=foo";
    var formattedCmd = formatter.formatCommand(cmd);

    var expected = "start locator --name=loc1 --J=\"-Dfoo=bar\" --J=\"-Dbar=foo\"";
    assertThat(formattedCmd).isEqualTo(expected);
  }

  @Test
  public void multipleJOptionsWithSomethingAfter() {
    var cmd = "start locator --name=loc1 --J=-Dfoo=bar --J=-Dbar=foo --group=locators";
    var formattedCmd = formatter.formatCommand(cmd);

    var expected =
        "start locator --name=loc1 --J=\"-Dfoo=bar\" --J=\"-Dbar=foo\" --group=locators";
    assertThat(formattedCmd).isEqualTo(expected);
  }

  @Test
  public void multipleJOptionsWithSomethingBetween() {
    var cmd = "start locator --name=loc1 --J=-Dfoo=bar --group=locators --J=-Dbar=foo";
    var formattedCmd = formatter.formatCommand(cmd);

    var expected =
        "start locator --name=loc1 --J=\"-Dfoo=bar\" --group=locators --J=\"-Dbar=foo\"";
    assertThat(formattedCmd).isEqualTo(expected);
  }

  @Test
  public void valueWithQuotes() {
    var cmd = "start locator --name=loc1 --J=\"-Dfoo=bar\"";
    var formattedCmd = formatter.formatCommand(cmd);
    assertThat(formattedCmd).isEqualTo(cmd);
  }

  @Test
  public void oneValueWithQuotesOneWithout() {
    var cmd = "start locator --name=loc1 --J=\"-Dfoo=bar\" --J=-Dfoo=bar";
    var formattedCmd = formatter.formatCommand(cmd);
    var expected = "start locator --name=loc1 --J=\"-Dfoo=bar\" --J=\"-Dfoo=bar\"";
    assertThat(formattedCmd).as(cmd).isEqualTo(expected);
  }

  @Test
  public void oneValueWithoutQuotesOneWith() {
    var cmd = "start locator --name=loc1 --J=-Dfoo=bar --J=\"-Dfoo=bar\"";
    var formattedCmd = formatter.formatCommand(cmd);
    var expected = "start locator --name=loc1 --J=\"-Dfoo=bar\" --J=\"-Dfoo=bar\"";
    assertThat(formattedCmd).isEqualTo(expected);
  }

  @Test
  public void twoValuesWithQuotes() {
    var cmd = "start locator --name=loc1 --J=\"-Dfoo=bar\" --J=\"-Dfoo=bar\"";
    var formattedCmd = formatter.formatCommand(cmd);
    assertThat(formattedCmd).as(cmd).isEqualTo(cmd);
  }

  @Test
  public void valueContainingQuotes() {
    var cmd = "start locator --name=loc1 --J=\"-Dfoo=region\"";
    var formattedCmd = formatter.formatCommand(cmd);
    var expected = "start locator --name=loc1 --J=\"-Dfoo=region\"";
    assertThat(formattedCmd).as(cmd).isEqualTo(expected);
  }

  @Test
  public void valueContainingQuotesAndSpace() {
    var cmd = "start locator --name=loc1 --J=\"-Dfoo=my phrase\"";
    var formattedCmd = formatter.formatCommand(cmd);
    var expected = "start locator --name=loc1 --J=\"-Dfoo=my phrase\"";
    assertThat(formattedCmd).as(cmd).isEqualTo(expected);
  }

  @Test
  public void valueContainingQuotesAndMultipleSpaces() {
    var cmd = "start locator --name=loc1 --J=\"-Dfoo=this is a phrase\"";
    var formattedCmd = formatter.formatCommand(cmd);
    var expected = "start locator --name=loc1 --J=\"-Dfoo=this is a phrase\"";
    assertThat(formattedCmd).as(cmd).isEqualTo(expected);
  }

  @Test
  public void valueContainingMultipleJWithSpaces() {
    var cmd =
        "start locator --name=loc1 --J=-Dfoo=this is a phrase             --J=\"-Dfoo=a short sentence\"";
    var formattedCmd = formatter.formatCommand(cmd);
    var expected =
        "start locator --name=loc1 --J=\"-Dfoo=this is a phrase\" --J=\"-Dfoo=a short sentence\"";
    assertThat(formattedCmd).as(cmd).isEqualTo(expected);
  }

  @Test
  public void valueContainingMultipleJWithSpaces2() {
    var cmd =
        "start locator --name=loc1 --J=\"-Dfoo=this is a phrase            \" --J=\"-Dfoo=a short sentence\"";
    var formattedCmd = formatter.formatCommand(cmd);
    var expected =
        "start locator --name=loc1 --J=\"-Dfoo=this is a phrase            \" --J=\"-Dfoo=a short sentence\"";
    assertThat(formattedCmd).as(cmd).isEqualTo(expected);
  }

  @Test
  public void optionAfterOneJOption() {
    var cmd = "start locator --name=loc1 --J=-Dfoo=bar --http-service=8080";
    var formattedCmd = formatter.formatCommand(cmd);
    var expected = "start locator --name=loc1 --J=\"-Dfoo=bar\" --http-service=8080";
    assertThat(formattedCmd).as(cmd).isEqualTo(expected);
  }

  @Test
  public void optionWithMoreThanOneHyphen() {
    var cmd = "start locator --name=loc1 --http-service-port=8080";
    var formattedCmd = formatter.formatCommand(cmd);
    var expected = "start locator --name=loc1 --http-service-port=8080";
    assertThat(formattedCmd).as(cmd).isEqualTo(expected);
  }

  @Test
  public void optionWithOneHyphenAfterOneJOption() {
    var cmd =
        "start server --name=me3 --J=-Dgemfire.jmx-manager=true --http-service-port=8080";
    var formattedCmd = formatter.formatCommand(cmd);
    var expected =
        "start server --name=me3 --J=\"-Dgemfire.jmx-manager=true\" --http-service-port=8080";
    assertThat(formattedCmd).as(cmd).isEqualTo(expected);
  }

  @Test // reproduces GEODE-2104
  public void optionWithMoreThanOneHyphenAfterOneJOption() {
    var cmd = "start server --name=me3 --J=-Dgemfire.jmx-manager=true --http-service-port=8080";
    var formattedCmd = formatter.formatCommand(cmd);
    var expected =
        "start server --name=me3 --J=\"-Dgemfire.jmx-manager=true\" --http-service-port=8080";
    assertThat(formattedCmd).as(cmd).isEqualTo(expected);
  }

  @Test
  public void optionWithOneHyphenAfterTwoJOptions() {
    var cmd =
        "start server --name=me3 --J=-Dgemfire.jmx-manager=true --J=-Dgemfire.jmx-manager-start=true --http-service-port=8080";
    var formattedCmd = formatter.formatCommand(cmd);
    var expected =
        "start server --name=me3 --J=\"-Dgemfire.jmx-manager=true\" --J=\"-Dgemfire.jmx-manager-start=true\" --http-service-port=8080";
    assertThat(formattedCmd).as(cmd).isEqualTo(expected);
  }

  @Test // reproduces GEODE-2104
  public void optionWithMoreThanOneHyphenAfterTwoJOptions() {
    var cmd =
        "start server --name=me3 --J=-Dgemfire.jmx-manager=true --J=-Dgemfire.jmx-manager-start=true --http-service-port=8080";
    var formattedCmd = formatter.formatCommand(cmd);
    var expected =
        "start server --name=me3 --J=\"-Dgemfire.jmx-manager=true\" --J=\"-Dgemfire.jmx-manager-start=true\" --http-service-port=8080";
    assertThat(formattedCmd).as(cmd).isEqualTo(expected);
  }

  @Test // reproduces GEODE-2075
  public void optionWithMoreThanOneHyphenWithoutValueAfterJOptions() {
    var cmd =
        "start server --name=Server2 --log-level=config --J=-Dgemfire.locators=localhost[10334] --disable-default-server";
    var formattedCmd = formatter.formatCommand(cmd);
    var expected =
        "start server --name=Server2 --log-level=config --J=\"-Dgemfire.locators=localhost[10334]\" --disable-default-server";
    assertThat(formattedCmd).as(cmd).isEqualTo(expected);
  }



}
