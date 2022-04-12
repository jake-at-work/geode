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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class StringRedactionTest {

  private static final String REDACTED = "redacted";

  private SensitiveDataDictionary sensitiveDataDictionary;
  private RedactionStrategy redactionStrategy;

  private StringRedaction stringRedaction;

  @Before
  public void setUp() {
    sensitiveDataDictionary = mock(SensitiveDataDictionary.class);
    redactionStrategy = mock(RedactionStrategy.class);

    stringRedaction =
        new StringRedaction(REDACTED, sensitiveDataDictionary, redactionStrategy);
  }

  @Test
  public void redactDelegatesString() {
    var input = "line";
    var expected = "expected";

    when(redactionStrategy.redact(input)).thenReturn(expected);

    var result = stringRedaction.redact(input);

    verify(redactionStrategy).redact(input);
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void redactDelegatesNullString() {
    String input = null;

    stringRedaction.redact(input);

    verify(redactionStrategy).redact(input);
  }

  @Test
  public void redactDelegatesEmptyString() {
    var input = "";

    stringRedaction.redact(input);

    verify(redactionStrategy).redact(input);
  }

  @Test
  public void redactDelegatesIterable() {
    var line1 = "line1";
    var line2 = "line2";
    var line3 = "line3";
    Collection<String> input = new ArrayList<>();
    input.add(line1);
    input.add(line2);
    input.add(line3);
    var joinedLine = String.join(" ", input);
    var expected = "expected";

    when(redactionStrategy.redact(joinedLine)).thenReturn(expected);

    var result = stringRedaction.redact(input);

    verify(redactionStrategy).redact(joinedLine);
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void redactNullIterableThrowsNullPointerException() {
    Collection<String> input = null;

    var thrown = catchThrowable(() -> {
      stringRedaction.redact(input);
    });

    assertThat(thrown).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void redactArgumentIfNecessaryDelegatesSensitiveKey() {
    var key = "key";
    var value = "value";

    when(sensitiveDataDictionary.isSensitive(key)).thenReturn(true);

    var result = stringRedaction.redactArgumentIfNecessary(key, value);

    verify(sensitiveDataDictionary).isSensitive(key);
    assertThat(result).isEqualTo(REDACTED);
  }

  @Test
  public void redactArgumentIfNecessaryDelegatesNonSensitiveKey() {
    var key = "key";
    var value = "value";

    when(sensitiveDataDictionary.isSensitive(key)).thenReturn(false);

    var result = stringRedaction.redactArgumentIfNecessary(key, value);

    verify(sensitiveDataDictionary).isSensitive(key);
    assertThat(result).isEqualTo(value);
  }

  @Test
  public void redactArgumentIfNecessaryDelegatesNullKey() {
    String key = null;

    stringRedaction.redactArgumentIfNecessary(key, "value");

    verify(sensitiveDataDictionary).isSensitive(key);
  }

  @Test
  public void redactArgumentIfNecessaryDelegatesEmptyKey() {
    var key = "";

    stringRedaction.redactArgumentIfNecessary(key, "value");

    verify(sensitiveDataDictionary).isSensitive(key);
  }

  @Test
  public void redactArgumentIfNecessaryReturnsNullValue() {
    String value = null;

    var result = stringRedaction.redactArgumentIfNecessary("key", value);

    assertThat(result).isEqualTo(value);
  }

  @Test
  public void redactArgumentIfNecessaryReturnsEmptyValue() {
    var value = "";

    var result = stringRedaction.redactArgumentIfNecessary("key", value);

    assertThat(result).isEqualTo(value);
  }

  @Test
  public void redactEachInListDelegatesEachStringInIterable() {
    var string1 = "string1";
    var string2 = "string2";
    var string3 = "string3";
    List<String> input = new ArrayList<>();
    input.add(string1);
    input.add(string2);
    input.add(string3);

    when(redactionStrategy.redact(anyString())).then(returnsFirstArg());

    var result = stringRedaction.redactEachInList(input);

    verify(redactionStrategy).redact(string1);
    verify(redactionStrategy).redact(string2);
    verify(redactionStrategy).redact(string3);
    assertThat(result).isEqualTo(input);
  }

  @Test
  public void redactEachInListDoesNotDelegateEmptyIterable() {
    List<String> input = Collections.emptyList();

    when(redactionStrategy.redact(anyString())).then(returnsFirstArg());

    var result = stringRedaction.redactEachInList(input);

    verifyNoInteractions(redactionStrategy);
    assertThat(result).isEqualTo(input);
  }

  @Test
  public void redactEachInListNullIterableThrowsNullPointerException() {
    List<String> input = null;

    when(redactionStrategy.redact(anyString())).then(returnsFirstArg());

    var thrown = catchThrowable(() -> {
      stringRedaction.redactEachInList(input);
    });

    assertThat(thrown).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void isSensitiveDelegatesString() {
    var input = "input";

    when(sensitiveDataDictionary.isSensitive(anyString())).thenReturn(true);

    var result = stringRedaction.isSensitive(input);

    assertThat(result).isTrue();
  }

  @Test
  public void isSensitiveDelegatesNullString() {
    String input = null;

    when(sensitiveDataDictionary.isSensitive(isNull())).thenReturn(true);

    var result = stringRedaction.isSensitive(input);

    assertThat(result).isTrue();
  }

  @Test
  public void isSensitiveDelegatesEmptyString() {
    var input = "";

    when(sensitiveDataDictionary.isSensitive(anyString())).thenReturn(true);

    var result = stringRedaction.isSensitive(input);

    assertThat(result).isTrue();
  }
}
