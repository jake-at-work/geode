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
package org.apache.geode.test.compiler;


import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.assertj.core.api.SoftAssertions;
import org.junit.Test;


public class ClassNameExtractorTest {
  private static final String SPACE = " ";
  private static final String CLASS_NAME_TO_FIND = "MyClassNameToFind";

  @Test
  public void extractsClassNames() throws Exception {
    var softAssertions = new SoftAssertions();
    var classNameExtractor = new ClassNameExtractor();

    var permutationsToTest = Sets.cartesianProduct(
        ImmutableSet.of("public ", "private ", "protected ", ""), ImmutableSet.of("abstract ", ""),
        ImmutableSet.of("static ", ""), ImmutableSet.of("class ", "interface "),
        ImmutableSet.of("extends Foo ", ""), ImmutableSet.of("implements Bar ", ""));

    for (var permutation : permutationsToTest) {
      var firstLineOfSource =
          permutation.get(0) + permutation.get(1) + permutation.get(2) + permutation.get(3)
              + CLASS_NAME_TO_FIND + SPACE + permutation.get(4) + permutation.get(5) + " {";

      var className = classNameExtractor.extractFromSourceCode(firstLineOfSource);
      softAssertions.assertThat(className).isEqualTo(CLASS_NAME_TO_FIND);
    }

    softAssertions.assertAll();
  }
}
