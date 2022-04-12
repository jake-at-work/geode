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
package org.apache.geode.internal.classloader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.test.compiler.ClassBuilder;
import org.apache.geode.test.junit.rules.ServerStarterRule;

/**
 * Integration tests for {@link ClassPathLoader}.
 */
public class ClassPathLoaderDeployTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public ServerStarterRule server = new ServerStarterRule();

  @After
  public void resetClassPathLoader() {
    ClassPathLoader.setLatestToDefault(null);
  }

  @Test
  public void testDeployWithExistingDependentJars() throws Exception {
    var classBuilder = new ClassBuilder();
    final var parentJarFile =
        new File("JarDeployerDUnitAParent.v1.jar");
    final var usesJarFile = new File("JarDeployerDUnitUses.v1.jar");
    final var functionJarFile =
        new File("JarDeployerDUnitFunction.v1.jar");

    // Write out a JAR files.
    var stringBuilder = new StringBuilder();
    stringBuilder.append("package jddunit.parent;");
    stringBuilder.append("public class JarDeployerDUnitParent {");
    stringBuilder.append("public String getValueParent() {");
    stringBuilder.append("return \"PARENT\";}}");

    var jarBytes = classBuilder.createJarFromClassContent(
        "jddunit/parent/JarDeployerDUnitParent", stringBuilder.toString());
    var outStream = new FileOutputStream(parentJarFile);
    outStream.write(jarBytes);
    outStream.close();

    stringBuilder = new StringBuilder();
    stringBuilder.append("package jddunit.uses;");
    stringBuilder.append("public class JarDeployerDUnitUses {");
    stringBuilder.append("public String getValueUses() {");
    stringBuilder.append("return \"USES\";}}");

    jarBytes = classBuilder.createJarFromClassContent("jddunit/uses/JarDeployerDUnitUses",
        stringBuilder.toString());
    outStream = new FileOutputStream(usesJarFile);
    outStream.write(jarBytes);
    outStream.close();

    stringBuilder = new StringBuilder();
    stringBuilder.append("package jddunit.function;");
    stringBuilder.append("import jddunit.parent.JarDeployerDUnitParent;");
    stringBuilder.append("import jddunit.uses.JarDeployerDUnitUses;");
    stringBuilder.append("import org.apache.geode.cache.execute.Function;");
    stringBuilder.append("import org.apache.geode.cache.execute.FunctionContext;");
    stringBuilder.append(
        "public class JarDeployerDUnitFunction  extends JarDeployerDUnitParent implements Function {");
    stringBuilder.append("private JarDeployerDUnitUses uses = new JarDeployerDUnitUses();");
    stringBuilder.append("public boolean hasResult() {return true;}");
    stringBuilder.append(
        "public void execute(FunctionContext context) {context.getResultSender().lastResult(getValueParent() + \":\" + uses.getValueUses());}");
    stringBuilder.append("public String getId() {return \"JarDeployerDUnitFunction\";}");
    stringBuilder.append("public boolean optimizeForWrite() {return false;}");
    stringBuilder.append("public boolean isHA() {return false;}}");

    var functionClassBuilder = new ClassBuilder();
    functionClassBuilder.addToClassPath(parentJarFile.getAbsolutePath());
    functionClassBuilder.addToClassPath(usesJarFile.getAbsolutePath());
    jarBytes = functionClassBuilder.createJarFromClassContent(
        "jddunit/function/JarDeployerDUnitFunction", stringBuilder.toString());
    outStream = new FileOutputStream(functionJarFile);
    outStream.write(jarBytes);
    outStream.close();

    server.startServer();

    var gemFireCache = GemFireCacheImpl.getInstance();
    var distributedSystem = gemFireCache.getDistributedSystem();
    var execution = FunctionService.onMember(distributedSystem.getDistributedMember());
    var resultCollector = execution.execute("JarDeployerDUnitFunction");
    var result = (List<String>) resultCollector.getResult();
    assertEquals("PARENT:USES", result.get(0));
  }

  @Test
  public void deployNewVersionOfFunctionOverOldVersion() throws Exception {
    var jarVersion1 = createVersionOfJar("Version1", "MyFunction", "MyJar.jar");
    var jarVersion2 = createVersionOfJar("Version2", "MyFunction", "MyJar.jar");

    server.startServer();

    GemFireCache gemFireCache = server.getCache();
    var distributedSystem = gemFireCache.getDistributedSystem();

    ClassPathLoader.getLatest().getJarDeploymentService()
        .deploy(createDeploymentFromJar(jarVersion1));

    assertThatClassCanBeLoaded("jddunit.function.MyFunction");
    var execution = FunctionService.onMember(distributedSystem.getDistributedMember());

    var result = (List<String>) execution.execute("MyFunction").getResult();
    assertThat(result.get(0)).isEqualTo("Version1");

    ClassPathLoader.getLatest().getJarDeploymentService()
        .deploy(createDeploymentFromJar(jarVersion2));
    result = (List<String>) execution.execute("MyFunction").getResult();
    assertThat(result.get(0)).isEqualTo("Version2");
  }

  private File createVersionOfJar(String version, String functionName, String jarName)
      throws IOException {
    var classContents =
        "package jddunit.function;" + "import org.apache.geode.cache.execute.Function;"
            + "import org.apache.geode.cache.execute.FunctionContext;" + "public class "
            + functionName + " implements Function {" + "public boolean hasResult() {return true;}"
            + "public String getId() {return \"" + functionName + "\";}"
            + "public void execute(FunctionContext context) {context.getResultSender().lastResult(\""
            + version + "\");}}";

    return createJarFromClassContents(version, functionName, jarName, classContents);
  }

  private File createJarFromClassContents(String version, String functionName, String jarName,
      String classContents)
      throws IOException {

    var jar = new File(temporaryFolder.newFolder(version), jarName);
    var classBuilder = new ClassBuilder();
    classBuilder.writeJarFromContent("jddunit/function/" + functionName, classContents,
        jar);

    return jar;
  }

  private File createJarFromClassContents(String version, String functionName, String jarName,
      String classContents, String additionalClassPath)
      throws IOException {

    var jar = new File(temporaryFolder.newFolder(version), jarName);
    var classBuilder = new ClassBuilder();
    classBuilder.addToClassPath(additionalClassPath);
    classBuilder.writeJarFromContent("jddunit/function/" + functionName, classContents,
        jar);

    return jar;
  }

  private void assertThatClassCanBeLoaded(String className) throws ClassNotFoundException {
    assertThat(ClassPathLoader.getLatest().forName(className)).isNotNull();
  }

  private Deployment createDeploymentFromJar(File jar) {
    var deployment = new Deployment(jar.getName(), "test", Instant.now().toString());
    deployment.setFile(jar);
    return deployment;
  }
}
