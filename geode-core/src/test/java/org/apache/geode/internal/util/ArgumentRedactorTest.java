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
package org.apache.geode.internal.util;

import static org.apache.geode.internal.util.ArgumentRedactor.getRedacted;
import static org.apache.geode.internal.util.ArgumentRedactor.isSensitive;
import static org.apache.geode.internal.util.ArgumentRedactor.redact;
import static org.apache.geode.internal.util.ArgumentRedactor.redactEachInList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Test;

public class ArgumentRedactorTest {

  @Test
  public void isSensitive_isTrueForGemfireSecurityPassword() {
    var input = "gemfire.security-password";

    var output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isTrue();
  }

  @Test
  public void isSensitive_isTrueForPassword() {
    var input = "password";

    var output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isTrue();
  }

  @Test
  public void isSensitive_isTrueForOptionContainingPassword() {
    var input = "other-password-option";

    var output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isTrue();
  }

  @Test
  public void isSensitive_isTrueForClusterSslTruststorePassword() {
    var input = "cluster-ssl-truststore-password";

    var output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isTrue();
  }

  @Test
  public void isSensitive_isTrueForGatewaySslTruststorePassword() {
    var input = "gateway-ssl-truststore-password";

    var output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isTrue();
  }

  @Test
  public void isSensitive_isTrueForServerSslKeystorePassword() {
    var input = "server-ssl-keystore-password";

    var output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isTrue();
  }

  @Test
  public void isSensitive_isTrueForSecurityUsername() {
    var input = "security-username";

    var output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isTrue();
  }

  @Test
  public void isSensitive_isTrueForSecurityManager() {
    var input = "security-manager";

    var output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isTrue();
  }

  @Test
  public void isSensitive_isTrueForOptionStartingWithSecurityHyphen() {
    var input = "security-important-property";

    var output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isTrue();
  }

  @Test
  public void isSensitive_isTrueForJavaxNetSslKeyStorePassword() {
    var input = "javax.net.ssl.keyStorePassword";

    var output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isTrue();
  }

  @Test
  public void isSensitive_isTrueForOptionStartingWithJavaxNetSsl() {
    var input = "javax.net.ssl.some.security.item";

    var output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isTrue();
  }

  @Test
  public void isSensitive_isTrueForJavaxNetSslKeyStoreType() {
    var input = "javax.net.ssl.keyStoreType";

    var output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isTrue();
  }

  @Test
  public void isSensitive_isTrueForOptionStartingWithSyspropHyphen() {
    var input = "sysprop-secret-prop";

    var output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isTrue();
  }

  @Test
  public void isSensitive_isFalseForGemfireSecurityManager() {
    var input = "gemfire.security-manager";

    var output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isFalse();
  }

  @Test
  public void isSensitive_isFalseForClusterSslEnabled() {
    var input = "cluster-ssl-enabled";

    var output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isFalse();
  }

  @Test
  public void isSensitive_isFalseForConserveSockets() {
    var input = "conserve-sockets";

    var output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isFalse();
  }

  @Test
  public void isSensitive_isFalseForUsername() {
    var input = "username";

    var output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isFalse();
  }

  @Test
  public void isSensitive_isFalseForNonMatchingStringContainingHyphens() {
    var input = "just-an-option";

    var output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isFalse();
  }

  @Test
  public void redactString_redactsGemfirePasswordWithHyphenD() {
    var string = "-Dgemfire.password=%s";
    var sensitive = "__this_should_be_redacted__";
    var input = String.format(string, sensitive);
    var expected = String.format(string, getRedacted());

    var output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsPasswordWithHyphens() {
    var string = "--password=%s";
    var sensitive = "__this_should_be_redacted__";
    var input = String.format(string, sensitive);
    var expected = String.format(string, getRedacted());

    var output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsOptionEndingWithPasswordWithHyphensJDd() {
    var string = "--J=-Dgemfire.some.very.qualified.item.password=%s";
    var sensitive = "__this_should_be_redacted__";
    var input = String.format(string, sensitive);
    var expected = String.format(string, getRedacted());

    var output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsOptionStartingWithSyspropHyphenWithHyphensJD() {
    var string = "--J=-Dsysprop-secret.information=%s";
    var sensitive = "__this_should_be_redacted__";
    var input = String.format(string, sensitive);
    var expected = String.format(string, getRedacted());

    var output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsGemfireSecurityPasswordWithHyphenD() {
    var string = "-Dgemfire.security-password=%s";
    var sensitive = "secret";
    var input = String.format(string, sensitive);
    var expected = String.format(string, getRedacted());

    var output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(expected);
  }

  @Test
  public void redactString_doesNotRedactOptionEndingWithSecurityPropertiesWithHyphenD1() {
    var input = "-Dgemfire.security-properties=argument-value";

    var output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(input);
  }

  @Test
  public void redactString_doesNotRedactOptionEndingWithSecurityPropertiesWithHyphenD2() {
    var input = "-Dgemfire.security-properties=\"c:\\Program Files (x86)\\My Folder\"";

    var output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(input);
  }

  @Test
  public void redactString_doesNotRedactOptionEndingWithSecurityPropertiesWithHyphenD3() {
    var input = "-Dgemfire.security-properties=./security-properties";

    var output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(input);
  }

  @Test
  public void redactString_doesNotRedactOptionContainingSecurityHyphenWithHyphensJD() {
    var input = "--J=-Dgemfire.sys.security-option=someArg";

    var output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(input);
  }

  @Test
  public void redactString_doesNotRedactNonMatchingGemfireOptionWithHyphenD() {
    var input = "-Dgemfire.sys.option=printable";

    var output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(input);
  }

  @Test
  public void redactString_redactsGemfireUseClusterConfigurationWithHyphenD() {
    var input = "-Dgemfire.use-cluster-configuration=true";

    var output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(input);
  }

  @Test
  public void redactString_returnsNonMatchingString() {
    var input = "someotherstringoption";

    var output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(input);
  }

  @Test
  public void redactString_doesNotRedactClasspathWithHyphens() {
    var input = "--classpath=.";

    var output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(input);
  }

  @Test
  public void redactString_redactsMatchingOptionWithNonMatchingOptionAndFlagAndMultiplePrefixes() {
    var string = "--J=-Dflag -Duser-password=%s --classpath=.";
    var sensitive = "foo";
    var input = String.format(string, sensitive);
    var expected = String.format(string, getRedacted());

    var output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsMultipleMatchingOptionsWithFlags() {
    var string = "-DmyArg -Duser-password=%s -DOtherArg -Dsystem-password=%s";
    var sensitive1 = "foo";
    var sensitive2 = "bar";
    var input = String.format(string, sensitive1, sensitive2);
    var expected = String.format(string, getRedacted(), getRedacted());

    var output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive1)
        .doesNotContain(sensitive2)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsMultipleMatchingOptionsWithMultipleNonMatchingOptionsAndMultiplePrefixes() {
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
        string, getRedacted(), nonSensitive, getRedacted(), getRedacted(), getRedacted());

    var output = redact(input);

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
  public void redactString_redactsMatchingOptionWithNonMatchingOptionAfterCommand() {
    var string = "connect --password=%s --user=%s";
    var reusedSensitive = "test";
    var input = String.format(string, reusedSensitive, reusedSensitive);
    var expected = String.format(string, getRedacted(), reusedSensitive);

    var output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .contains(reusedSensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsMultipleMatchingOptionsButNotKeyUsingSameStringAsValue() {
    var string = "connect --%s-password=%s --product-password=%s";
    var reusedSensitive = "test";
    var sensitive = "test1";
    var input = String.format(string, reusedSensitive, reusedSensitive, sensitive);
    var expected = String.format(string, reusedSensitive, getRedacted(), getRedacted());

    var output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .contains(reusedSensitive)
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactRedactsGemfireSslTruststorePassword() {
    var string = "-Dgemfire.ssl-truststore-password=%s";
    var sensitive = "gibberish";
    var input = String.format(string, sensitive);
    var expected = String.format(string, getRedacted());

    var output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsGemfireSslKeystorePassword() {
    var string = "-Dgemfire.ssl-keystore-password=%s";
    var sensitive = "gibberish";
    var input = String.format(string, sensitive);
    var expected = String.format(string, getRedacted());

    var output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsValueEndingWithHyphen() {
    var string = "-Dgemfire.ssl-keystore-password=%s";
    var sensitive = "supersecret-";
    var input = String.format(string, sensitive);
    var expected = String.format(string, getRedacted());

    var output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsValueContainingHyphen() {
    var string = "-Dgemfire.ssl-keystore-password=%s";
    var sensitive = "super-secret";
    var input = String.format(string, sensitive);
    var expected = String.format(string, getRedacted());

    var output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsValueContainingManyHyphens() {
    var string = "-Dgemfire.ssl-keystore-password=%s";
    var sensitive = "this-is-super-secret";
    var input = String.format(string, sensitive);
    var expected = String.format(string, getRedacted());

    var output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsValueStartingWithHyphen() {
    var string = "-Dgemfire.ssl-keystore-password=%s";
    var sensitive = "-supersecret";
    var input = String.format(string, sensitive);
    var expected = String.format(string, getRedacted());

    var output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsQuotedValueStartingWithHyphen() {
    var string = "-Dgemfire.ssl-keystore-password=%s";
    var sensitive = "\"-supersecret\"";
    var input = String.format(string, sensitive);
    var expected = String.format(string, getRedacted());

    var output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactIterable_redactsMultipleMatchingOptions() {
    var sensitive1 = "secret";
    var sensitive2 = "super-secret";
    var sensitive3 = "confidential";
    var sensitive4 = "shhhh";
    var sensitive5 = "failed";

    Collection<String> input = new ArrayList<>();
    input.add("--gemfire.security-password=" + sensitive1);
    input.add("--login-password=" + sensitive1);
    input.add("--gemfire-password = " + sensitive2);
    input.add("--geode-password= " + sensitive3);
    input.add("--some-other-password =" + sensitive4);
    input.add("--justapassword =" + sensitive5);

    var output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive1)
        .doesNotContain(sensitive2)
        .doesNotContain(sensitive3)
        .doesNotContain(sensitive4)
        .doesNotContain(sensitive5)
        .contains("--gemfire.security-password=" + getRedacted())
        .contains("--login-password=" + getRedacted())
        .contains("--gemfire-password = " + getRedacted())
        .contains("--geode-password= " + getRedacted())
        .contains("--some-other-password =" + getRedacted())
        .contains("--justapassword =" + getRedacted());
  }

  @Test
  public void redactIterable_doesNotRedactMultipleNonMatchingOptions() {
    Collection<String> input = new ArrayList<>();
    input.add("--gemfire.security-properties=./security.properties");
    input.add("--gemfire.sys.security-option=someArg");
    input.add("--gemfire.use-cluster-configuration=true");
    input.add("--someotherstringoption");
    input.add("--login-name=admin");
    input.add("--myArg --myArg2 --myArg3=-arg4");
    input.add("--myArg --myArg2 --myArg3=\"-arg4\"");

    var output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .contains("--gemfire.security-properties=./security.properties")
        .contains("--gemfire.sys.security-option=someArg")
        .contains("--gemfire.use-cluster-configuration=true")
        .contains("--someotherstringoption")
        .contains("--login-name=admin")
        .contains("--myArg --myArg2 --myArg3=-arg4")
        .contains("--myArg --myArg2 --myArg3=\"-arg4\"");
  }

  @Test
  public void redactEachInList_redactsCollectionOfMatchingOptions() {
    var sensitive1 = "secret";
    var sensitive2 = "super-secret";
    var sensitive3 = "confidential";
    var sensitive4 = "shhhh";
    var sensitive5 = "failed";

    Collection<String> input = new ArrayList<>();
    input.add("--gemfire.security-password=" + sensitive1);
    input.add("--login-password=" + sensitive1);
    input.add("--gemfire-password = " + sensitive2);
    input.add("--geode-password= " + sensitive3);
    input.add("--some-other-password =" + sensitive4);
    input.add("--justapassword =" + sensitive5);

    var output = redactEachInList(input);

    assertThat(output)
        .as("output of redactEachInList(" + input + ")")
        .doesNotContain(sensitive1)
        .doesNotContain(sensitive2)
        .doesNotContain(sensitive3)
        .doesNotContain(sensitive4)
        .doesNotContain(sensitive5)
        .contains("--gemfire.security-password=" + getRedacted())
        .contains("--login-password=" + getRedacted())
        .contains("--gemfire-password = " + getRedacted())
        .contains("--geode-password= " + getRedacted())
        .contains("--some-other-password =" + getRedacted())
        .contains("--justapassword =" + getRedacted());
  }
}
