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
package org.apache.geode.management.internal.cli;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.shell.event.ParseResult;

import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

@Category({GfshTest.class})
public class GfshParserParsingTest {
  @ClassRule
  public static GfshParserRule parser = new GfshParserRule();
  private String buffer;


  private GfshParseResult parseParams(String input, String commandMethod) {
    ParseResult parseResult = parser.parse(input);

    var gfshParseResult = (GfshParseResult) parseResult;

    assertThat(gfshParseResult.getMethod().getName()).isEqualTo(commandMethod);
    assertThat(gfshParseResult.getUserInput()).isEqualTo(input.trim());

    return gfshParseResult;
  }

  @Test
  public void getSimpleParserInputTest() {
    buffer = "start locator  --J=\"-Dgemfire.http-service-port=8080\" --name=loc1";
    assertEquals("start locator --J \"-Dgemfire.http-service-port=8080\" --name loc1",
        GfshParser.convertToSimpleParserInput(buffer));

    buffer = "start locator --J=-Dgemfire.http-service-port=8080 --name=loc1 --J=-Ddummythinghere";
    assertEquals("start locator --J \"-Dgemfire.http-service-port=8080"
        + GfshParser.J_ARGUMENT_DELIMITER + "-Ddummythinghere\" --name loc1",
        GfshParser.convertToSimpleParserInput(buffer));

    buffer = "start locator --";
    assertThat(GfshParser.convertToSimpleParserInput(buffer)).isEqualTo("start locator --");

    buffer =
        "start locator --J=-Dgemfire.http-service-port=8080 --name=loc1 --J=-Ddummythinghere --";
    assertEquals("start locator --J \"-Dgemfire.http-service-port=8080"
        + GfshParser.J_ARGUMENT_DELIMITER + "-Ddummythinghere\" --name loc1 --",
        GfshParser.convertToSimpleParserInput(buffer));

    buffer = "start server --name=name1 --locators=localhost --J=-Dfoo=bar";
    assertEquals("start server --name name1 --locators localhost --J \"-Dfoo=bar\"",
        GfshParser.convertToSimpleParserInput(buffer));
  }

  @Test
  public void testStartLocatorJOptionWithComma() throws Exception {
    buffer =
        "start locator --name=test --J='-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000' --J=-Dfoo=bar";
    var result = parser.parse(buffer);
    assertThat(result).isNotNull();
    var arguments = result.getArguments();
    var indexOfJvmArgumentsParameterInStartLocator = 18;

    var jvmArgs = (String[]) arguments[indexOfJvmArgumentsParameterInStartLocator];
    assertThat(jvmArgs).hasSize(2);

    // make sure the resulting jvm arguments do not have quotes (either single or double) around
    // them.
    assertThat(jvmArgs[0])
        .isEqualTo("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000");
    assertThat(jvmArgs[1]).isEqualTo("-Dfoo=bar");
  }

  @Test
  public void testStartServerJOptionWithComma() throws Exception {
    buffer =
        "start server --name=test --J='-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000' --J='-Dfoo=bar'";
    var result = parser.parse(buffer);
    assertThat(result).isNotNull();
    var arguments = result.getArguments();
    var indexOfJvmArgumentsParameterInStartServer = 19;
    var jvmArgs = (String[]) arguments[indexOfJvmArgumentsParameterInStartServer];
    assertThat(jvmArgs).hasSize(2);

    // make sure the resulting jvm arguments do not have quotes (either single or double) around
    // them.
    assertThat(jvmArgs[0])
        .isEqualTo("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000");
    assertThat(jvmArgs[1]).isEqualTo("-Dfoo=bar");
  }

  @Test
  public void testStartJConsoleJOptionWithComma() throws Exception {
    buffer =
        "start jconsole --J='-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000' --J=-Dfoo=bar";
    var result = parser.parse(buffer);
    assertThat(result).isNotNull();
    var arguments = result.getArguments();
    // the 4th argument is the jvmarguments;
    var jvmArgs = (String[]) arguments[4];
    assertThat(jvmArgs).hasSize(2);

    // make sure the resulting jvm arguments do not have quotes (either single or double) around
    // them.
    assertThat(jvmArgs[0])
        .isEqualTo("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000");
    assertThat(jvmArgs[1]).isEqualTo("-Dfoo=bar");
  }

  @Test
  public void testStartJvisulvmOptionWithComma() throws Exception {
    buffer =
        "start jvisualvm --J=\"-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000\" --J=-Dfoo=bar";
    var result = parser.parse(buffer);
    assertThat(result).isNotNull();
    var arguments = result.getArguments();
    // the 1st argument is the jvmarguments;
    var jvmArgs = (String[]) arguments[0];
    assertThat(jvmArgs).hasSize(2);

    // make sure the resulting jvm arguments do not have quotes (either single or double) around
    // them.
    assertThat(jvmArgs[0])
        .isEqualTo("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=30000");
    assertThat(jvmArgs[1]).isEqualTo("-Dfoo=bar");
  }

  @Test
  public void testParseOptionStartsWithHyphenWithoutQuotes() throws Exception {
    var input =
        "rebalance --exclude-region=" + SEPARATOR
            + "GemfireDataCommandsDUnitTestRegion2 --simulate=true --time-out=-1";
    var result = parseParams(input, "rebalance");
    assertThat(result.getParamValueAsString("exclude-region"))
        .isEqualTo(SEPARATOR + "GemfireDataCommandsDUnitTestRegion2");
    assertThat(result.getParamValueAsString("simulate")).isEqualTo("true");
    assertThat(result.getParamValueAsString("time-out")).isEqualTo("-1");
  }

  @Test
  public void testParseOptionStartsWithHyphenWithQuotes() throws Exception {
    var input =
        "rebalance --exclude-region=" + SEPARATOR
            + "GemfireDataCommandsDUnitTestRegion2 --simulate=true --time-out=\"-1\"";
    var result = parseParams(input, "rebalance");

    assertThat(result.getParamValueAsString("exclude-region"))
        .isEqualTo(SEPARATOR + "GemfireDataCommandsDUnitTestRegion2");
    assertThat(result.getParamValueAsString("simulate")).isEqualTo("true");
    assertThat(result.getParamValueAsString("time-out")).isEqualTo("-1");
  }

  @Test
  public void testParseOptionContainingHyphen() throws Exception {
    var input = "rebalance --exclude-region=" + SEPARATOR + "The-Region --simulate=true";
    var result = parseParams(input, "rebalance");

    assertThat(result.getParamValueAsString("exclude-region")).isEqualTo(SEPARATOR + "The-Region");
    assertThat(result.getParamValueAsString("simulate")).isEqualTo("true");
  }

  @Test
  public void testParseOptionContainingUnderscore() throws Exception {
    var input = "rebalance --exclude-region=" + SEPARATOR + "The_region --simulate=true";
    var result = parseParams(input, "rebalance");

    assertThat(result.getParamValueAsString("exclude-region")).isEqualTo(SEPARATOR + "The_region");
    assertThat(result.getParamValueAsString("simulate")).isEqualTo("true");
  }

  @Test
  public void testParseOneJOptionWithQuotes() throws Exception {
    var input = "start locator  --J=\"-Dgemfire.http-service-port=8080\" --name=loc1";
    var result = parseParams(input, "startLocator");

    assertThat(result.getParamValueAsString("name")).isEqualTo("loc1");
    assertThat(result.getParamValueAsString("J")).isEqualTo("-Dgemfire.http-service-port=8080");
  }

  @Test
  public void testParseOneJOptionWithSpaceInQuotes() throws Exception {
    var input = "start locator  --J=\"-Dgemfire.http-service-port= 8080\" --name=loc1";
    var result = parseParams(input, "startLocator");

    assertThat(result.getParamValueAsString("name")).isEqualTo("loc1");
    assertThat(result.getParamValueAsString("J")).isEqualTo("-Dgemfire.http-service-port= 8080");
  }

  @Test
  public void testParseOneJOption() throws Exception {
    var input = "start locator --J=-Dgemfire.http-service-port=8080 --name=loc1";
    var result = parseParams(input, "startLocator");

    assertThat(result.getParamValueAsString("name")).isEqualTo("loc1");
    assertThat(result.getParamValueAsString("J")).isEqualTo("-Dgemfire.http-service-port=8080");
  }

  @Test
  public void testParseTwoJOptions() throws Exception {
    var input =
        "start locator --J=-Dgemfire.http-service-port=8080 --name=loc1 --J=-Ddummythinghere";
    var result = parseParams(input, "startLocator");

    assertThat(result.getParamValueAsString("name")).isEqualTo("loc1");
    assertThat(result.getParamValueAsString("J"))
        .isEqualTo("-Dgemfire.http-service-port=8080,-Ddummythinghere");
  }

  @Test
  public void testParseTwoJOptionsOneWithQuotesOneWithout() throws Exception {
    var input =
        "start locator --J=\"-Dgemfire.http-service-port=8080\" --name=loc1 --J=-Ddummythinghere";
    var result = parseParams(input, "startLocator");

    assertThat(result.getParamValueAsString("name")).isEqualTo("loc1");
    assertThat(result.getParamValueAsString("J"))
        .isEqualTo("-Dgemfire.http-service-port=8080,-Ddummythinghere");
  }

  @Test
  public void testParseOneJOptionWithQuotesAndLotsOfSpaces() throws Exception {
    var input =
        "start locator       --J=\"-Dgemfire.http-service-port=8080\"      --name=loc1         ";
    var result = parseParams(input, "startLocator");

    assertThat(result.getParamValueAsString("name")).isEqualTo("loc1");
    assertThat(result.getParamValueAsString("J")).isEqualTo("-Dgemfire.http-service-port=8080");
  }

  @Test
  public void testObtainHelp() {
    var command = CliStrings.START_PULSE;
    var helpString = ("NAME\n" + "start pulse\n" + "IS AVAILABLE\n" + "true\n" + "SYNOPSIS\n"
        + "Open a new window in the default Web browser with the URL for the Pulse application.\n"
        + "SYNTAX\n" + "start pulse [--url=value]\n" + "PARAMETERS\n" + "url\n"
        + "URL of the Pulse Web application.\n" + "Required: false\n"
        + "Default (if the parameter is not specified): http://localhost:7070/pulse\n").replace(
            "\n", System.lineSeparator());
    assertThat(parser.getCommandManager().obtainHelp(command)).isEqualTo(helpString);
  }

  @Test
  public void testDeployCommand() throws Exception {
    var command = "deploy --jar=/tmp/junit7552412945092669041/jar1.jar";
    var result = parser.parse(command);
    assertThat(result).isNotNull();
  }


  @Test
  public void testCommandWithBackSlash() throws Exception {
    var command =
        "describe offline-disk-store --name=testDiskStore --disk-dirs=R:\\regrResults\\test";
    var result = parser.parse(command);
    assertThat(result.getParamValueAsString("disk-dirs")).isEqualTo("R:\\regrResults\\test");
  }

  @Test
  public void testCommandWithBackSlashTwo() throws Exception {
    var command = "start locator --name=\\test";
    var result = parser.parse(command);
    assertThat(result.getParamValueAsString("name")).isEqualTo("\\test");
  }

  @Test
  public void testCommandWithBackSlashThree() throws Exception {
    var command = "start locator --name=\\myName";
    var result = parser.parse(command);
    assertThat(result.getParamValueAsString("name")).isEqualTo("\\myName");
  }

  @Test
  public void testCommandWithBackSlashFour() throws Exception {
    var command = "start locator --name=\\u0005Name";
    var result = parser.parse(command);
    assertThat(result.getParamValueAsString("name")).isEqualTo("\\u0005Name");
  }

  @Test
  public void testValueOfJsonWithoutOuterQuoteAndSpace() throws Exception {
    var command = "put --key=('name':'id') --value=456 --region=" + SEPARATOR + "test";
    var result = parser.parse(command);
    assertThat(result.getParamValueAsString("key")).isEqualTo("('name':'id')");
  }

  @Test
  public void testValueOfJsonWithSpace() throws Exception {
    // this is considerred an invalid command
    var command = "put --key=('name' : 'id') --value=456 --region=" + SEPARATOR + "test";
    var result = parser.parse(command);
    assertThat(result).isNull();
  }

  @Test
  public void testValueOfJsonWithSpaceAndOuterQuotes() throws Exception {
    var command = "put --key=\"('name' : 'id')\" --value=456 --region=" + SEPARATOR + "test";
    var result = parser.parse(command);
    assertThat(result.getParamValueAsString("key")).isEqualTo("('name' : 'id')");
  }

  @Test
  public void optionValueWillNotBeTrimmedIfInQuotes() throws Exception {
    var command = "start locator --name=' test '";
    var result = parser.parse(command);
    assertThat(result.getParamValueAsString("name")).isEqualTo(" test ");
  }

  @Test
  public void optionValueWithExtraSpaceInBetween() throws Exception {
    var command = "start locator --name= test    --bind-address=123";
    var result = parser.parse(command);
    assertThat(result.getParamValueAsString("name")).isEqualTo("test");
    assertThat(result.getParamValueAsString("bind-address")).isEqualTo("123");
  }

  @Test
  public void optionValueWithEmptyString() throws Exception {
    var command = "start locator --name= --bind-address=123";
    var result = parser.parse(command);
    assertThat(result.getParamValueAsString("name")).isNull();
    assertThat(result.getParamValueAsString("bind-address")).isEqualTo("123");
  }

  @Test
  public void optionValueWithQuotedEmptyString() throws Exception {
    var command = "start locator --name='' --bind-address=123";
    var result = parser.parse(command);
    assertThat(result.getParamValueAsString("name")).isNull();
    assertThat(result.getParamValueAsString("bind-address")).isEqualTo("123");
  }

  @Test
  public void testMultiLineCommand() throws Exception {
    var command = "start server " + GfshParser.LINE_SEPARATOR + "--name=test";
    var result = parser.parse(command);
    assertThat(result.getParamValueAsString("name")).isEqualTo("test");
    assertThat(result.getCommandName()).isEqualTo("start server");
  }

  @Test
  public void testShutdownWithOptionCommand() throws Exception {
    var command = "shutdown --include-locators";
    var result = parser.parse(command);
    assertThat(result.getParamValue("include-locators")).isEqualTo(true);
  }
}
