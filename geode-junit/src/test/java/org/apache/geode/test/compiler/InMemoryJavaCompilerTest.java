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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import org.junit.Test;

public class InMemoryJavaCompilerTest {
  private final InMemoryJavaCompiler compiler = new InMemoryJavaCompiler();

  @Test
  public void compileSingleClass() {
    var sourceCode = "package test.pkg;"
        + "public class MyClassName {}";

    var classFiles = compiler.compile(sourceCode);

    assertThat(classFiles)
        .extracting(InMemoryClassFile::getName)
        .containsExactly("test.pkg.MyClassName");
  }

  @Test
  public void compileTwoDependentClasses() {
    var baseClassSourceCode = "package test.pkg;"
        + "public class BaseClass {}";
    var derivedClassSourceCode = "package test.pkg;"
        + "public class DerivedClass extends BaseClass {}";

    var classFiles =
        compiler.compile(derivedClassSourceCode, baseClassSourceCode);

    assertThat(classFiles)
        .extracting(InMemoryClassFile::getName)
        .containsExactlyInAnyOrder("test.pkg.BaseClass", "test.pkg.DerivedClass");
  }

  @Test
  public void invalidSourceThrowsException() {
    var invalidSourceCode = ("public class foo {this is not valid java source code}");
    var thrown = catchThrowable(() -> compiler.compile(invalidSourceCode));
    assertThat(thrown)
        .hasMessageContaining(invalidSourceCode);
  }
}
