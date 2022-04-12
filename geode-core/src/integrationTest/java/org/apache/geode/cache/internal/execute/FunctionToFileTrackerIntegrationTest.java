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

package org.apache.geode.cache.internal.execute;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.classloader.ClassPathLoader;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.test.compiler.ClassBuilder;

public class FunctionToFileTrackerIntegrationTest {

  private final ClassBuilder classBuilder = new ClassBuilder();

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void registerFunctions() throws IOException {
    var functionJar = registerFunctionJar();

    var deployment = createDeploymentFromJar(functionJar);
    ClassPathLoader.getLatest().getJarDeploymentService().deploy(deployment);

    var registeredFunctions = FunctionService.getRegisteredFunctions().keySet();
    assertThat(registeredFunctions.size()).isEqualTo(1);
    assertThat(registeredFunctions).containsExactly("JarClassLoaderJUnitFunction");
  }

  private File registerFunctionJar() throws IOException {
    final var parentJarFile = temporaryFolder.newFile("JarClassLoaderJUnitParent.jar");
    final var usesJarFile = temporaryFolder.newFile("JarClassLoaderJUnitUses.jar");

    // Write out a JAR files.
    var StringBuilder = new StringBuilder();
    StringBuilder.append("package jcljunit.parent;");
    StringBuilder.append("public class JarClassLoaderJUnitParent {");
    StringBuilder.append("public String getValueParent() {");
    StringBuilder.append("return \"PARENT\";}}");

    var jarBytes = classBuilder.createJarFromClassContent(
        "jcljunit/parent/JarClassLoaderJUnitParent", StringBuilder.toString());
    writeJarBytesToFile(parentJarFile, jarBytes);
    var parentDeployment = createDeploymentFromJar(parentJarFile);
    ClassPathLoader.getLatest().getJarDeploymentService().deploy(parentDeployment);

    StringBuilder = new StringBuilder();
    StringBuilder.append("package jcljunit.uses;");
    StringBuilder.append("public class JarClassLoaderJUnitUses {");
    StringBuilder.append("public String getValueUses() {");
    StringBuilder.append("return \"USES\";}}");

    jarBytes = classBuilder.createJarFromClassContent("jcljunit/uses/JarClassLoaderJUnitUses",
        StringBuilder.toString());
    writeJarBytesToFile(usesJarFile, jarBytes);
    var userDeployment = createDeploymentFromJar(usesJarFile);
    ClassPathLoader.getLatest().getJarDeploymentService().deploy(userDeployment);

    StringBuilder = new StringBuilder();
    StringBuilder.append("package jcljunit.function;");
    StringBuilder.append("import jcljunit.parent.JarClassLoaderJUnitParent;");
    StringBuilder.append("import jcljunit.uses.JarClassLoaderJUnitUses;");
    StringBuilder.append("import org.apache.geode.cache.execute.Function;");
    StringBuilder.append("import org.apache.geode.cache.execute.FunctionContext;");
    StringBuilder.append(
        "public class JarClassLoaderJUnitFunction  extends JarClassLoaderJUnitParent implements Function {");
    StringBuilder.append("private JarClassLoaderJUnitUses uses = new JarClassLoaderJUnitUses();");
    StringBuilder.append("public boolean hasResult() {return true;}");
    StringBuilder.append(
        "public void execute(FunctionContext context) {context.getResultSender().lastResult(getValueParent() + \":\" + uses.getValueUses());}");
    StringBuilder.append("public String getId() {return \"JarClassLoaderJUnitFunction\";}");
    StringBuilder.append("public boolean optimizeForWrite() {return false;}");
    StringBuilder.append("public boolean isHA() {return false;}}");

    var functionClassBuilder = new ClassBuilder();
    functionClassBuilder.addToClassPath(parentJarFile.getAbsolutePath());
    functionClassBuilder.addToClassPath(usesJarFile.getAbsolutePath());
    jarBytes = functionClassBuilder.createJarFromClassContent(
        "jcljunit/function/JarClassLoaderJUnitFunction", StringBuilder.toString());
    var functionJar = temporaryFolder.newFile("JarClassLoaderJUnitFunction.jar");
    writeJarBytesToFile(functionJar, jarBytes);
    return functionJar;
  }

  @Test
  public void unregisterFunctionsForDeployment() throws IOException {
    var functionJar = registerFunctionJar();

    var deployment = createDeploymentFromJar(functionJar);
    ClassPathLoader.getLatest().getJarDeploymentService().deploy(deployment);

    var registeredFunctions = FunctionService.getRegisteredFunctions().keySet();
    assertThat(registeredFunctions.size()).isEqualTo(1);
    assertThat(registeredFunctions).containsExactly("JarClassLoaderJUnitFunction");

    ClassPathLoader.getLatest().getJarDeploymentService()
        .undeployByFileName(deployment.getFileName());
    registeredFunctions = FunctionService.getRegisteredFunctions().keySet();
    assertThat(registeredFunctions.size()).isEqualTo(0);
  }

  private Deployment createDeploymentFromJar(File jar) {
    var deployment = new Deployment(jar.getName(), "test", Instant.now().toString());
    deployment.setFile(jar);
    return deployment;
  }

  private void writeJarBytesToFile(File jarFile, byte[] jarBytes) throws IOException {
    final OutputStream outStream = new FileOutputStream(jarFile);
    outStream.write(jarBytes);
    outStream.close();
  }
}
