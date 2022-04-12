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

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class JarBuilderTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private JarBuilder jarBuilder;
  private File outputJar;

  @Before
  public void setup() throws IOException {
    jarBuilder = new JarBuilder();
    outputJar = new File(temporaryFolder.getRoot(), "output.jar");
  }

  @Test
  public void jarWithSingleClass() throws Exception {
    var classContents = loadTestResource("AbstractClass.java");
    jarBuilder.buildJar(outputJar, classContents);

    var jarEntryNames = jarEntryNamesFromFile(outputJar);
    assertThat(jarEntryNames).containsExactlyInAnyOrder(
        "timestamp",
        "org/apache/geode/test/compiler/AbstractClass.class");
  }

  @Test
  public void jarWithTwoDependentClasses() throws Exception {
    var sourceFileOne = loadTestResource("AbstractClass.java");
    var sourceFileTwo = loadTestResource("ConcreteClass.java");

    jarBuilder.buildJar(outputJar, sourceFileOne, sourceFileTwo);

    var jarEntryNames = jarEntryNamesFromFile(outputJar);

    assertThat(jarEntryNames).containsExactlyInAnyOrder(
        "timestamp",
        "org/apache/geode/test/compiler/AbstractClass.class",
        "org/apache/geode/test/compiler/ConcreteClass.class");
  }

  @Test
  public void jarWithClassInDefaultPackage() throws Exception {
    var classInFooBarPackage = "package foo.bar; public class ClassInFooBarPackage {}";
    var classInDefaultPackage = "public class ClassInDefaultPackage {}";
    jarBuilder.buildJar(outputJar, classInFooBarPackage, classInDefaultPackage);

    var jarEntryNames = jarEntryNamesFromFile(outputJar);
    assertThat(jarEntryNames).containsExactlyInAnyOrder(
        "timestamp",
        "ClassInDefaultPackage.class",
        "foo/bar/ClassInFooBarPackage.class");
  }


  @Test
  public void jarFromOnlyClassNames() throws Exception {
    var defaultPackageClassName = "DefaultClass";
    var otherPackageClassName = "foo.bar.OtherClass";
    jarBuilder.buildJarFromClassNames(outputJar, defaultPackageClassName, otherPackageClassName);

    var jarEntryNames = jarEntryNamesFromFile(outputJar);
    assertThat(jarEntryNames).containsExactlyInAnyOrder("DefaultClass.class",
        "foo/bar/OtherClass.class", "timestamp");
  }

  @Test
  public void canLoadClassesFromJar() throws Exception {
    var defaultPackageClassName = "DefaultClass";
    var otherPackageClassName = "foo.bar.OtherClass";
    jarBuilder.buildJarFromClassNames(outputJar, defaultPackageClassName, otherPackageClassName);

    var jarClassLoader = new URLClassLoader(new URL[] {outputJar.toURL()});

    jarClassLoader.loadClass("DefaultClass");
    jarClassLoader.loadClass("foo.bar.OtherClass");
  }

  private Set<String> jarEntryNamesFromFile(File jarFile) throws Exception {
    assertThat(jarFile).exists();

    var jarEntries = new JarFile(jarFile).entries();
    return Collections.list(jarEntries).stream().map(JarEntry::getName).collect(toSet());
  }

  private File loadTestResource(String fileName) throws URISyntaxException {
    var resourceFileURL = getClass().getResource(fileName);
    assertThat(resourceFileURL).isNotNull();

    var resourceUri = resourceFileURL.toURI();
    return new File(resourceUri);
  }
}
