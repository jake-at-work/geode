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
package org.apache.geode.internal.util.redaction;

import static org.apache.geode.internal.util.redaction.RedactionDefaults.SENSITIVE_PREFIXES;
import static org.apache.geode.internal.util.redaction.RedactionDefaults.SENSITIVE_SUBSTRINGS;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

public class RegexRedactionStrategyTest {

  private static final String REDACTED = "redacted";

  private RegexRedactionStrategy regexRedactionStrategy;

  @Before
  public void setUp() {
    SensitiveDataDictionary sensitiveDataDictionary = new CombinedSensitiveDictionary(
        new SensitivePrefixDictionary(SENSITIVE_PREFIXES),
        new SensitiveSubstringDictionary(SENSITIVE_SUBSTRINGS));

    regexRedactionStrategy =
        new RegexRedactionStrategy(sensitiveDataDictionary::isSensitive, REDACTED);
  }

  @Test
  public void redactsGemfirePasswordWithHyphenD() {
    var string = "-Dgemfire.password=%s";
    var sensitive = "__this_should_be_redacted__";
    var input = String.format(string, sensitive);
    var expected = String.format(string, REDACTED);

    var output = regexRedactionStrategy.redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactsPasswordWithHyphens() {
    var string = "--password=%s";
    var sensitive = "__this_should_be_redacted__";
    var input = String.format(string, sensitive);
    var expected = String.format(string, REDACTED);

    var output = regexRedactionStrategy.redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactsOptionEndingWithPasswordWithHyphensJDd() {
    var string = "--J=-Dgemfire.some.very.qualified.item.password=%s";
    var sensitive = "__this_should_be_redacted__";
    var input = String.format(string, sensitive);
    var expected = String.format(string, REDACTED);

    var output = regexRedactionStrategy.redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactsOptionStartingWithSyspropHyphenWithHyphensJD() {
    var string = "--J=-Dsysprop-secret.information=%s";
    var sensitive = "__this_should_be_redacted__";
    var input = String.format(string, sensitive);
    var expected = String.format(string, REDACTED);

    var output = regexRedactionStrategy.redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactsGemfireSecurityPasswordWithHyphenD() {
    var string = "-Dgemfire.security-password=%s";
    var sensitive = "secret";
    var input = String.format(string, sensitive);
    var expected = String.format(string, REDACTED);

    var output = regexRedactionStrategy.redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(expected);
  }

  @Test
  public void doesNotRedactOptionEndingWithSecurityPropertiesWithHyphenD1() {
    var input = "-Dgemfire.security-properties=argument-value";

    var output = regexRedactionStrategy.redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(input);
  }

  @Test
  public void doesNotRedactOptionEndingWithSecurityPropertiesWithHyphenD2() {
    var input = "-Dgemfire.security-properties=\"c:\\Program Files (x86)\\My Folder\"";

    var output = regexRedactionStrategy.redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(input);
  }

  @Test
  public void doesNotRedactOptionEndingWithSecurityPropertiesWithHyphenD3() {
    var input = "-Dgemfire.security-properties=./security-properties";

    var output = regexRedactionStrategy.redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(input);
  }

  @Test
  public void doesNotRedactOptionContainingSecurityHyphenWithHyphensJD() {
    var input = "--J=-Dgemfire.sys.security-option=someArg";

    var output = regexRedactionStrategy.redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(input);
  }

  @Test
  public void doesNotRedactNonMatchingGemfireOptionWithHyphenD() {
    var input = "-Dgemfire.sys.option=printable";

    var output = regexRedactionStrategy.redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(input);
  }

  @Test
  public void redactsGemfireUseClusterConfigurationWithHyphenD() {
    var input = "-Dgemfire.use-cluster-configuration=true";

    var output = regexRedactionStrategy.redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(input);
  }

  @Test
  public void returnsNonMatchingString() {
    var input = "someotherstringoption";

    var output = regexRedactionStrategy.redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(input);
  }

  @Test
  public void doesNotRedactClasspathWithHyphens() {
    var input = "--classpath=.";

    var output = regexRedactionStrategy.redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(input);
  }

  @Test
  public void redactsMatchingOptionWithNonMatchingOptionAndFlagAndMultiplePrefixes() {
    var string = "--J=-Dflag -Duser-password=%s --classpath=.";
    var sensitive = "foo";
    var input = String.format(string, sensitive);
    var expected = String.format(string, REDACTED);

    var output = regexRedactionStrategy.redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactsMultipleMatchingOptionsWithFlags() {
    var string = "-DmyArg -Duser-password=%s -DOtherArg -Dsystem-password=%s";
    var sensitive1 = "foo";
    var sensitive2 = "bar";
    var input = String.format(string, sensitive1, sensitive2);
    var expected = String.format(string, REDACTED, REDACTED);

    var output = regexRedactionStrategy.redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive1)
        .doesNotContain(sensitive2)
        .isEqualTo(expected);
  }

  @Test
  public void redactsMultipleMatchingOptionsWithMultipleNonMatchingOptionsAndMultiplePrefixes() {
    var string =
        "-Dlogin-password=%s -Dlogin-name=%s -Dgemfire-password = %s --geode-password= %s --J=-Dsome-other-password =%s";
    var sensitive1 = "secret";
    var nonSensitive = "admin";
    var sensitive2 = "super-secret";
    var sensitive3 = "confidential";
    var sensitive4 = "shhhh";
    var input = String.format(
        string, sensitive1, nonSensitive, sensitive2, sensitive3, sensitive4);
    var expected = String.format(
        string, REDACTED, nonSensitive, REDACTED, REDACTED, REDACTED);

    var output = regexRedactionStrategy.redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive1)
        .contains(nonSensitive)
        .doesNotContain(sensitive2)
        .doesNotContain(sensitive3)
        .doesNotContain(sensitive4)
        .isEqualTo(expected);
  }

  @Test
  public void redactsMatchingOptionWithNonMatchingOptionAfterCommand() {
    var string = "connect --password=%s --user=%s";
    var reusedSensitive = "test";
    var input = String.format(string, reusedSensitive, reusedSensitive);
    var expected = String.format(string, REDACTED, reusedSensitive);

    var output = regexRedactionStrategy.redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .contains(reusedSensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactsMultipleMatchingOptionsButNotKeyUsingSameStringAsValue() {
    var string = "connect --%s-password=%s --product-password=%s";
    var reusedSensitive = "test";
    var sensitive = "test1";
    var input = String.format(string, reusedSensitive, reusedSensitive, sensitive);
    var expected = String.format(string, reusedSensitive, REDACTED, REDACTED);

    var output = regexRedactionStrategy.redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .contains(reusedSensitive)
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactRedactsGemfireSslTruststorePassword() {
    var string = "-Dgemfire.ssl-truststore-password=%s";
    var sensitive = "gibberish";
    var input = String.format(string, sensitive);
    var expected = String.format(string, REDACTED);

    var output = regexRedactionStrategy.redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactsGemfireSslKeystorePassword() {
    var string = "-Dgemfire.ssl-keystore-password=%s";
    var sensitive = "gibberish";
    var input = String.format(string, sensitive);
    var expected = String.format(string, REDACTED);

    var output = regexRedactionStrategy.redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactsValueEndingWithHyphen() {
    var string = "-Dgemfire.ssl-keystore-password=%s";
    var sensitive = "supersecret-";
    var input = String.format(string, sensitive);
    var expected = String.format(string, REDACTED);

    var output = regexRedactionStrategy.redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactsValueContainingHyphen() {
    var string = "-Dgemfire.ssl-keystore-password=%s";
    var sensitive = "super-secret";
    var input = String.format(string, sensitive);
    var expected = String.format(string, REDACTED);

    var output = regexRedactionStrategy.redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactsValueContainingManyHyphens() {
    var string = "-Dgemfire.ssl-keystore-password=%s";
    var sensitive = "this-is-super-secret";
    var input = String.format(string, sensitive);
    var expected = String.format(string, REDACTED);

    var output = regexRedactionStrategy.redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactsValueStartingWithHyphen() {
    var string = "-Dgemfire.ssl-keystore-password=%s";
    var sensitive = "-supersecret";
    var input = String.format(string, sensitive);
    var expected = String.format(string, REDACTED);

    var output = regexRedactionStrategy.redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactsQuotedValueStartingWithHyphen() {
    var string = "-Dgemfire.ssl-keystore-password=%s";
    var sensitive = "\"-supersecret\"";
    var input = String.format(string, sensitive);
    var expected = String.format(string, REDACTED);

    var output = regexRedactionStrategy.redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }
}
