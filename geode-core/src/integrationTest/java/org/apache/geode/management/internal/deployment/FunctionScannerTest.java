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
package org.apache.geode.management.internal.deployment;

import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.test.compiler.JarBuilder;
import org.apache.geode.test.junit.categories.GfshTest;

@Category({GfshTest.class})
public class FunctionScannerTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private JarBuilder jarBuilder;
  private FunctionScanner functionScanner;
  private File outputJar;

  @Before
  public void setup() throws IOException {
    jarBuilder = new JarBuilder();
    functionScanner = new FunctionScanner();
    outputJar = new File(temporaryFolder.getRoot(), "output.jar");
  }

  @Test
  public void implementsFunction() throws Exception {
    var sourceFileOne = loadTestResource("ImplementsFunction.java");

    jarBuilder.buildJar(outputJar, sourceFileOne);

    var functionsFoundInJar = functionScanner.findFunctionsInJar(outputJar);
    assertThat(functionsFoundInJar)
        .contains("org.apache.geode.management.internal.deployment.ImplementsFunction");
  }

  @Test
  public void extendsFunctionAdapter() throws Exception {
    var sourceFileOne = loadTestResource("ExtendsFunctionAdapter.java");

    jarBuilder.buildJar(outputJar, sourceFileOne);

    var functionsFoundInJar = functionScanner.findFunctionsInJar(outputJar);
    assertThat(functionsFoundInJar)
        .contains("org.apache.geode.management.internal.deployment.ExtendsFunctionAdapter");
  }

  @Test
  public void testConcreteExtendsAbstractExtendsFunctionAdapter() throws Exception {
    var sourceFileOne = loadTestResource("AbstractExtendsFunctionAdapter.java");
    var sourceFileTwo = loadTestResource("ConcreteExtendsAbstractExtendsFunctionAdapter.java");

    jarBuilder.buildJar(outputJar, sourceFileOne, sourceFileTwo);

    var functionsFoundInJar = functionScanner.findFunctionsInJar(outputJar);
    assertThat(functionsFoundInJar).contains(
        "org.apache.geode.management.internal.deployment.ConcreteExtendsAbstractExtendsFunctionAdapter",
        "org.apache.geode.management.internal.deployment.AbstractExtendsFunctionAdapter");
  }

  @Test
  public void testConcreteExtendsAbstractImplementsFunction() throws Exception {
    var sourceFileOne = loadTestResource("AbstractImplementsFunction.java");
    var sourceFileTwo = loadTestResource("ConcreteExtendsAbstractImplementsFunction.java");

    jarBuilder.buildJar(outputJar, sourceFileOne, sourceFileTwo);

    var functionsFoundInJar = functionScanner.findFunctionsInJar(outputJar);
    assertThat(functionsFoundInJar).contains(
        "org.apache.geode.management.internal.deployment.ConcreteExtendsAbstractImplementsFunction",
        "org.apache.geode.management.internal.deployment.AbstractImplementsFunction");
  }

  @Test
  @Ignore("Fails due to GEODE-3429")
  public void registerFunctionHierarchySplitAcrossTwoJars() throws Exception {
    var sourceFileOne = loadTestResource("AbstractImplementsFunction.java");
    var abstractJar = new File(temporaryFolder.getRoot(), "abstract.jar");
    jarBuilder.buildJar(abstractJar, sourceFileOne);

    jarBuilder.addToClasspath(abstractJar);
    var sourceFileTwo = loadTestResource("AnnotatedFunction.java");

    jarBuilder.buildJar(outputJar, sourceFileTwo);
    var functionsFoundInJar = functionScanner.findFunctionsInJar(outputJar);
    assertThat(functionsFoundInJar).contains(
        "org.apache.geode.management.internal.deployment.AnnotatedFunction");
  }

  private File loadTestResource(String fileName) {
    var filePath =
        createTempFileFromResource(getClass(), fileName).getAbsolutePath();
    assertThat(filePath).isNotNull();

    return new File(filePath);
  }

}
