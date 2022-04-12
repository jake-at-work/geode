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

import static java.lang.System.lineSeparator;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.shell.core.Completion;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.management.internal.cli.commands.StartServerCommand;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

@Category(GfshTest.class)
public class GfshParserAutoCompletionIntegrationTest {

  private static int startServerCommandCliOptions = 0;

  @BeforeClass
  public static void calculateStartServerCommandParameters() {
    Object o = new StartServerCommand();
    for (var method : o.getClass().getDeclaredMethods()) {
      if (method.getName().equals("startServer")) {
        for (var param : method.getParameters()) {
          var annotation = param.getAnnotation(CliOption.class);
          startServerCommandCliOptions += annotation.key().length;
        }
        break;
      }
    }
    assertThat(startServerCommandCliOptions).isNotZero();
  }

  @AfterClass
  public static void cleanup() {
    startServerCommandCliOptions = 0;
  }

  @Rule
  public GfshParserRule gfshParserRule = new GfshParserRule();

  @Test
  public void testCompletionDescribe() {
    var buffer = "describe";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(9);
    assertThat(candidate.getFirstCandidate()).isEqualTo("describe client");
  }

  @Test
  public void testCompletionDescribeWithSpace() {
    var buffer = "describe ";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(9);
    assertThat(candidate.getFirstCandidate()).isEqualTo("describe client");
  }

  @Test
  public void testCompletionDeploy() {
    var buffer = "deploy";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()
        .stream().map(completion -> completion.getValue().trim()).collect(Collectors.toList()))
            .containsExactlyInAnyOrder("--dir", "--jar", "--jars", "--group", "--groups");
  }

  @Test
  public void testCompletionDeployWithSpace() {
    var buffer = "deploy ";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()
        .stream().map(completion -> completion.getValue().trim()).collect(Collectors.toList()))
            .containsExactlyInAnyOrder("--dir", "--jar", "--jars", "--group", "--groups");
  }

  @Test
  public void testCompleteWithRequiredOption() {
    var buffer = "describe config";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(1);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + " --member");
  }

  @Test
  public void testCompleteWithRequiredOptionWithSpace() {
    var buffer = "describe config ";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(1);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "--member");
  }

  @Test
  public void testCompletionStart() {
    var buffer = "start";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates().size()).isEqualTo(8);
    assertThat(candidate.getCandidates().stream()
        .anyMatch(completion -> completion.getFormattedValue().contains("gateway-receiver")))
            .isTrue();
    assertThat(candidate.getCandidates().stream()
        .anyMatch(completion -> completion.getFormattedValue().contains("vsd")))
            .isTrue();
  }

  @Test
  public void testCompletionStartWithSpace() {
    var buffer = "start ";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates().size()).isEqualTo(8);
    assertThat(candidate.getCandidates().stream()
        .anyMatch(completion -> completion.getFormattedValue().contains("gateway-receiver")))
            .isTrue();
    assertThat(candidate.getCandidates().stream()
        .anyMatch(completion -> completion.getFormattedValue().contains("vsd")))
            .isTrue();
  }

  @Test
  public void testCompleteCommand() {
    var buffer = "start ser";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(1);
    assertThat("start server").isEqualTo(candidate.getFirstCandidate());
  }

  @Test
  public void testCompleteOptionWithOnlyOneCandidate() {
    var buffer = "start server --nam";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(1);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "e");
  }

  @Test
  public void testCompleteOptionWithMultipleCandidates() {
    var buffer = "start server --name=jinmei --loc";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(3);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "ator-wait-time");
    assertThat(candidate.getCandidate(1)).isEqualTo(buffer + "ators");
    assertThat(candidate.getCandidate(2)).isEqualTo(buffer + "k-memory");
  }

  @Test
  public void testCompleteWithExtraSpace() {
    var buffer = "start server --name=name1  --se";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo("start server --name=name1  ".length());
    assertThat(candidate.getCandidates()).hasSize(3);
    assertThat(candidate.getCandidates()).contains(new Completion("--server-port"));
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "curity-properties-file");
  }

  @Test
  public void testCompleteWithDashInTheEnd() {
    var buffer = "start server --name=name1 --";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length() - 2);
    assertThat(candidate.getCandidates()).hasSize(startServerCommandCliOptions - 1);
    assertThat(candidate.getCandidates()).contains(new Completion("--properties-file"));
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "J");
  }

  @Test
  public void testCompleteWithSpace() {
    var buffer = "start server --name=name1 ";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length() - 1);
    assertThat(candidate.getCandidates()).hasSize(startServerCommandCliOptions - 1);
    assertThat(candidate.getCandidates()).contains(new Completion(" --properties-file"));
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "--J");
  }

  @Test
  public void testCompleteWithOutSpace() {
    var buffer = "start server --name=name1";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length());
    assertThat(candidate.getCandidates()).hasSize(startServerCommandCliOptions - 1);
    assertThat(candidate.getCandidates()).contains(new Completion(" --properties-file"));
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + " --J");
  }

  @Test
  public void testCompleteJ() {
    var buffer = "start server --name=name1 --J=";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length() - 3);
    assertThat(candidate.getCandidates()).hasSize(1);
  }

  @Test
  public void testCompleteWithValue() {
    var buffer = "start server --name=name1 --J";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length() - 3);
    assertThat(candidate.getCandidates()).hasSize(1);
  }

  @Test
  public void testCompleteWithDash() {
    var buffer = "start server --name=name1 --J=-Dfoo.bar --";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(startServerCommandCliOptions - 2);
  }

  @Test
  public void testCompleteWithMultipleJ() {
    var buffer = "start server --name=name1 --J=-Dme=her --J=-Dfoo=bar --l";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCursor())
        .isEqualTo("start server --name=name1 --J=-Dme=her --J=-Dfoo=bar ".length());
    assertThat(candidate.getCandidates()).hasSize(4);
    assertThat(candidate.getCandidates()).contains(new Completion("--locators"));
  }

  @Test
  public void testMultiJComplete() {
    var buffer = "start server --name=name1 --J=-Dtest=test1 --J=-Dfoo=bar";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length());
    assertThat(candidate.getCandidates()).hasSize(startServerCommandCliOptions - 2);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + " --assign-buckets");
  }

  @Test
  public void testMultiJCompleteWithDifferentOrder() {
    var buffer = "start server --J=-Dtest=test1 --J=-Dfoo=bar --name=name1";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length());
    assertThat(candidate.getCandidates()).hasSize(startServerCommandCliOptions - 2);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + " --assign-buckets");
  }

  @Test
  public void testJComplete3() {
    var buffer = "start server --name=name1 --locators=localhost --J=-Dfoo=bar";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length());
    assertThat(candidate.getCandidates()).hasSize(startServerCommandCliOptions - 3);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + " --assign-buckets");
  }

  @Test
  public void testJComplete4() {
    var buffer = "start server --name=name1 --locators=localhost  --J=-Dfoo=bar --";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCursor()).isEqualTo(buffer.length() - 2);
    assertThat(candidate.getCandidates()).hasSize(startServerCommandCliOptions - 3);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "assign-buckets");
  }

  @Test
  public void testCompleteRegionType() {
    var buffer = "create region --name=test --type";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(23);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "=LOCAL");
  }

  @Test
  public void testCompletePartialRegionType() {
    var buffer = "create region --name=test --type=LO";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(5);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "CAL");
  }

  @Test
  public void testCompleteWithRegionTypeWithNoSpace() {
    var buffer = "create region --name=test --type=REPLICATE";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(5);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "_HEAP_LRU");
  }

  @Test
  public void testCompleteWithRegionTypeWithSpace() {
    var buffer = "create region --name=test --type=REPLICATE ";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(46);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "--async-event-queue-id");
  }

  @Test
  public void testCompleteLogLevel() {
    var buffer = "change loglevel --loglevel";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(8);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "=ALL");
  }

  @Test
  public void testCompleteLogLevelWithEqualSign() {
    var buffer = "change loglevel --loglevel=";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(8);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "ALL");
  }

  @Test
  public void testCompleteHintNonexistemt() {
    var buffer = "hint notfound";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(0);
  }

  @Test
  public void testCompleteHintNada() {
    var buffer = "hint";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates().size()).isGreaterThan(10);
    assertThat(candidate.getFirstCandidate()).isEqualToIgnoringCase("hint client");
  }

  @Test
  public void testCompleteHintSpace() {
    var buffer = "hint ";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates().size()).isGreaterThan(10);
    assertThat(candidate.getFirstCandidate()).isEqualToIgnoringCase("hint client");
  }

  @Test
  public void testCompleteHintPartial() {
    var buffer = "hint d";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(3);
    assertThat(candidate.getFirstCandidate()).isEqualToIgnoringCase("hint data");
  }

  @Test
  public void testCompleteHintAlreadyComplete() {
    var buffer = "hint data";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(1);
    assertThat(candidate.getFirstCandidate()).isEqualToIgnoringCase(buffer);
  }

  @Test
  public void testCompleteHelpFirstWord() {
    var buffer = "help start";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(8);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + " gateway-receiver");
  }

  @Test
  public void testCompleteHelpPartialFirstWord() {
    var buffer = "help st";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(18);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "art gateway-receiver");
  }

  @Test
  public void testObtainHelp() {
    var command = CliStrings.START_PULSE;
    var helpString = "NAME" + lineSeparator() + "start pulse" + lineSeparator() + "IS AVAILABLE"
        + lineSeparator() + "true" + lineSeparator() + "SYNOPSIS" + lineSeparator()
        + "Open a new window in the default Web browser with the URL for the Pulse application."
        + lineSeparator()
        + "SYNTAX" + lineSeparator() + "start pulse [--url=value]" + lineSeparator() + "PARAMETERS"
        + lineSeparator() + "url" + lineSeparator()
        + "URL of the Pulse Web application." + lineSeparator() + "Required: false"
        + lineSeparator()
        + "Default (if the parameter is not specified): http://localhost:7070/pulse"
        + lineSeparator();
    assertThat(gfshParserRule.getCommandManager().obtainHelp(command)).isEqualTo(helpString);
  }

  @Test
  public void testObtainHelpForStart() {
    var command = "start";
    var helpProvided = gfshParserRule.getCommandManager().getHelper().getHelp(command, 1000);
    var helpProvidedArray = helpProvided.split(lineSeparator());
    assertThat(helpProvidedArray.length).isEqualTo(8 * 2 + 3);
    for (var i = 0; i < helpProvidedArray.length - 3; i++) {
      if (i % 2 != 0) {
        assertThat(helpProvidedArray[i]).startsWith("    ");
      } else {
        assertThat(helpProvidedArray[i]).startsWith(command);
      }
    }
  }

  @Test
  public void testObtainHintForData() {
    var hintArgument = "data";
    var hintsProvided = gfshParserRule.getCommandManager().obtainHint(hintArgument);
    var hintsProvidedArray = hintsProvided.split(lineSeparator());
    assertThat(hintsProvidedArray).hasSize(17);
    assertThat(hintsProvidedArray[0])
        .isEqualTo("User data as stored in regions of the Geode distributed system.");
  }

  @Test
  public void testObtainHintWithoutArgument() {
    var hintArgument = "";
    var hintsProvided = gfshParserRule.getCommandManager().obtainHint(hintArgument);
    var hintsProvidedArray = hintsProvided.split(lineSeparator());
    assertThat(hintsProvidedArray).hasSize(21);
    assertThat(hintsProvidedArray[0]).isEqualTo(
        "Hints are available for the following topics. Use \"hint <topic-name>\" for a specific hint.");
  }

  @Test
  public void testObtainHintWithNonExistingCommand() {
    var hintArgument = "fortytwo";
    var hintsProvided = gfshParserRule.getCommandManager().obtainHint(hintArgument);
    var hintsProvidedArray = hintsProvided.split(lineSeparator());
    assertThat(hintsProvidedArray).hasSize(1);
    assertThat(hintsProvidedArray[0]).isEqualTo(
        "Unknown topic: " + hintArgument + ". Use hint to view the list of available topics.");
  }

  @Test
  public void testObtainHintWithPartialCommand() {
    var hintArgument = "d";
    var hintsProvided = gfshParserRule.getCommandManager().obtainHint(hintArgument);
    System.out.println(hintsProvided);
    var hintsProvidedArray = hintsProvided.split(lineSeparator());
    assertThat(hintsProvidedArray.length).isEqualTo(5);
    assertThat(hintsProvidedArray[0]).isEqualTo(
        "Hints are available for the following topics. Use \"hint <topic-name>\" for a specific hint.");
    assertThat(hintsProvidedArray).contains("Data");
    assertThat(hintsProvidedArray).contains("Debug-Utility");
    assertThat(hintsProvidedArray).contains("Disk Store");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testIndexType() {
    var buffer = "create index --type=";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates())
        .hasSize(org.apache.geode.cache.query.IndexType.values().length);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "hash");
  }

  @Test
  public void testCompletionOffersMandatoryOptionsInAlphabeticalOrderForCreateGatewaySenderWithSpace() {
    var buffer = "create gateway-sender ";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(2);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "--id");
    assertThat(candidate.getCandidates().get(1).getValue())
        .isEqualTo(buffer + "--remote-distributed-system-id");
  }

  @Test
  public void testCompletionOffersTheFirstMandatoryOptionInAlphabeticalOrderForCreateGatewaySenderWithDash() {
    var buffer = "create gateway-sender --";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(1);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "id");
  }

  @Test
  public void testCompletionOffersMandatoryOptionsInAlphabeticalOrderForChangeLogLevelWithSpace() {
    var buffer = "change loglevel ";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(1);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "--loglevel");
  }

  @Test
  public void testCompletionOffersTheFirstMandatoryOptionInAlphabeticalOrderForChangeLogLevelWithDash() {
    var buffer = "change loglevel --";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(1);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "loglevel");
  }

  @Test
  public void testCompletionOffersMandatoryOptionsInAlphabeticalOrderForCreateDiskStoreWithSpace() {
    var buffer = "create disk-store ";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(2);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "--dir");
    assertThat(candidate.getCandidate(1)).isEqualTo(buffer + "--name");
  }

  @Test
  public void testCompletionOffersTheFirstMandatoryOptionInAlphabeticalOrderForCreateDiskStoreWithDash() {
    var buffer = "create disk-store --";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(1);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "dir");
  }

  @Test
  public void testCompletionOffersMandatoryOptionsInAlphabeticalOrderForCreateJndiBindingWithSpace() {
    var buffer = "create jndi-binding ";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(3);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "--connection-url");
    assertThat(candidate.getCandidate(1)).isEqualTo(buffer + "--name");
    assertThat(candidate.getCandidate(2)).isEqualTo(buffer + "--url");
  }

  @Test
  public void testCompletionOffersTheFirstMandatoryOptionInAlphabeticalOrderForCreateJndiBindingWithDash() {
    var buffer = "create jndi-binding --";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(2);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "connection-url");
    assertThat(candidate.getCandidate(1)).isEqualTo(buffer + "url");
  }

  @Test
  public void testCompletionOffersMandatoryOptionsInAlphabeticalOrderForDestroyGwSenderWithSpace() {
    var buffer = "destroy gateway-sender ";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(1);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "--id");
  }

  @Test
  public void testCompletionOffersTheFirstMandatoryOptionInAlphabeticalOrderForDestroyGwSenderWithDash() {
    var buffer = "destroy gateway-sender --";

    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(1);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "id");
  }

  @Test
  public void testCompletionOffersMandatoryOptionsInAlphabeticalOrderForExportDataWithSpace() {
    var buffer = "export data ";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(2);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "--member");
    assertThat(candidate.getCandidate(1)).isEqualTo(buffer + "--region");
  }

  @Test
  public void testCompletionOffersTheFirstMandatoryOptionInAlphabeticalOrderForExportDataWithDash() {
    var buffer = "export data --";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(1);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "member");
  }

  @Test
  public void testCompletionOffersMandatoryOptionsInAlphabeticalOrderForImportDataWithSpace() {
    var buffer = "import data ";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(2);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "--member");
    assertThat(candidate.getCandidate(1)).isEqualTo(buffer + "--region");
  }

  @Test
  public void testCompletionOffersTheFirstMandatoryOptionInAlphabeticalOrderForImportDataWithDash() {
    var buffer = "import data --";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(1);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "member");
  }

  @Test
  public void testCompletionOffersMandatoryOptionsInAlphabeticalOrderForRemoveWithSpace() {
    var buffer = "remove ";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(1);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "--region");
  }

  @Test
  public void testCompletionOffersTheFirstMandatoryOptionInAlphabeticalOrderForRemoveWithDash() {
    var buffer = "remove --";
    var candidate = gfshParserRule.complete(buffer);
    assertThat(candidate.getCandidates()).hasSize(1);
    assertThat(candidate.getFirstCandidate()).isEqualTo(buffer + "region");
  }

}
