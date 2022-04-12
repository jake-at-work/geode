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
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

public class CombinedSensitiveDictionaryTest {

  @Test
  public void isFalseWhenZeroDelegates() {
    var combined = new CombinedSensitiveDictionary();

    var result = combined.isSensitive("string");

    assertThat(result).isFalse();
  }

  @Test
  public void delegatesInputToSingleDictionary() {
    var input = "string";
    var dictionary = mock(SensitiveDataDictionary.class);
    var combined = new CombinedSensitiveDictionary(dictionary);

    combined.isSensitive(input);

    verify(dictionary).isSensitive(same(input));
  }

  @Test
  public void delegatesInputToTwoDictionaries() {
    var input = "string";
    var dictionary1 = mock(SensitiveDataDictionary.class);
    var dictionary2 = mock(SensitiveDataDictionary.class);
    var combined =
        new CombinedSensitiveDictionary(dictionary1, dictionary2);

    combined.isSensitive(input);

    verify(dictionary1).isSensitive(same(input));
    verify(dictionary2).isSensitive(same(input));
  }

  @Test
  public void delegatesInputToManyDictionaries() {
    var input = "string";
    var dictionary1 = mock(SensitiveDataDictionary.class);
    var dictionary2 = mock(SensitiveDataDictionary.class);
    var dictionary3 = mock(SensitiveDataDictionary.class);
    var dictionary4 = mock(SensitiveDataDictionary.class);
    var combined =
        new CombinedSensitiveDictionary(dictionary1, dictionary2, dictionary3, dictionary4);

    combined.isSensitive(input);

    verify(dictionary1).isSensitive(same(input));
    verify(dictionary2).isSensitive(same(input));
    verify(dictionary3).isSensitive(same(input));
    verify(dictionary4).isSensitive(same(input));
  }

  @Test
  public void isFalseWhenManyDictionariesAreFalse() {
    var input = "string";
    var dictionary1 = createDictionary(false);
    var dictionary2 = createDictionary(false);
    var dictionary3 = createDictionary(false);
    var dictionary4 = createDictionary(false);
    var combined =
        new CombinedSensitiveDictionary(dictionary1, dictionary2, dictionary3, dictionary4);

    var result = combined.isSensitive(input);

    assertThat(result).isFalse();
  }

  @Test
  public void isTrueWhenManyDictionariesAreTrue() {
    var input = "string";
    var dictionary1 = createDictionary(true);
    var dictionary2 = createDictionary(true);
    var dictionary3 = createDictionary(true);
    var dictionary4 = createDictionary(true);
    var combined =
        new CombinedSensitiveDictionary(dictionary1, dictionary2, dictionary3, dictionary4);

    var result = combined.isSensitive(input);

    assertThat(result).isTrue();
  }

  @Test
  public void isTrueWhenOneOfManyDictionariesIsTrue() {
    var input = "string";
    var dictionary1 = createDictionary(false);
    var dictionary2 = createDictionary(false);
    var dictionary3 = createDictionary(false);
    var dictionary4 = createDictionary(true);
    var combined =
        new CombinedSensitiveDictionary(dictionary1, dictionary2, dictionary3, dictionary4);

    var result = combined.isSensitive(input);

    assertThat(result).isTrue();
  }

  private SensitiveDataDictionary createDictionary(boolean isSensitive) {
    var dictionary = mock(SensitiveDataDictionary.class);
    when(dictionary.isSensitive(anyString())).thenReturn(isSensitive);
    return dictionary;
  }
}
