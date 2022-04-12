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
package org.apache.geode.test.dunit.internal;

import static java.util.stream.Collectors.joining;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION;
import static org.apache.geode.distributed.internal.DistributionConfig.MEMBERSHIP_PORT_RANGE_NAME;
import static org.apache.geode.test.process.JavaModuleHelper.getJvmModuleOptions;
import static org.apache.geode.util.internal.GeodeGlossary.GEMFIRE_PREFIX;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.apache.commons.io.FileUtils;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.membership.utils.AvailablePort;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.version.VersionManager;

class ProcessManager implements ChildVMLauncher {
  private final int namingPort;
  private final Map<Integer, ProcessHolder> processes = new HashMap<>();
  private File log4jConfig;
  private int pendingVMs;
  private final Registry registry;
  private int debugPort = Integer.getInteger("dunit.debug.basePort", 0);
  private final int suspendVM = Integer.getInteger("dunit.debug.suspendVM", -100);
  private final VersionManager versionManager;

  public ProcessManager(int namingPort, Registry registry) {
    versionManager = VersionManager.getInstance();
    this.namingPort = namingPort;
    this.registry = registry;
  }

  public synchronized void launchVM(int vmNum) throws IOException {
    launchVM(VersionManager.CURRENT_VERSION, vmNum, false, 0);
  }

  @Override
  public synchronized ProcessHolder launchVM(String version, int vmNum, boolean bouncedVM,
      int remoteStubPort) throws IOException {
    if (bouncedVM) {
      processes.remove(vmNum);
    }
    if (processes.containsKey(vmNum)) {
      throw new IllegalStateException("VM " + vmNum + " is already running.");
    }

    var workingDir = getVMDir(version, vmNum);
    if (!workingDir.exists()) {
      workingDir.mkdirs();
    } else if (!bouncedVM || DUnitLauncher.MAKE_NEW_WORKING_DIRS) {
      try {
        FileUtils.deleteDirectory(workingDir);
      } catch (IOException e) {
        // This delete is occasionally failing on some platforms, maybe due to a lingering
        // process. Allow the process to be launched anyway.
        System.err.println("Unable to delete " + workingDir + ". Currently contains "
            + Arrays.asList(workingDir.list()));
      }
      workingDir.mkdirs();
    }

    var cmd = buildJavaCommand(vmNum, namingPort, version, remoteStubPort);
    System.out.println("Executing " + Arrays.toString(cmd));

    if (log4jConfig != null) {
      FileUtils.copyFileToDirectory(log4jConfig, workingDir);
    }

    // TODO - delete directory contents, preferably with commons io FileUtils
    try {
      String[] envp = null;
      if (!VersionManager.isCurrentVersion(version)) {
        envp = new String[] {"GEODE_HOME=" + versionManager.getInstall(version)};
      }
      var process = Runtime.getRuntime().exec(cmd, envp, workingDir);
      pendingVMs++;
      var holder = new ProcessHolder(process);
      processes.put(vmNum, holder);
      linkStreams(version, vmNum, holder, holder.getErrorStream(), System.err);
      linkStreams(version, vmNum, holder, holder.getInputStream(), System.out);
      return holder;
    } catch (RuntimeException | Error t) {
      t.printStackTrace();
      throw t;
    }
  }

  public void validateVersion(String version) {
    if (!versionManager.isValidVersion(version)) {
      throw new IllegalArgumentException("Version " + version + " is not configured for use");
    }
  }

  public static File getVMDir(String version, int vmNum) {
    return new File(DUnitLauncher.DUNIT_DIR, VM.getVMName(VersionManager.CURRENT_VERSION, vmNum));
  }

  public synchronized void killVMs() {
    for (var process : processes.values()) {
      if (process != null) {
        process.kill();
      }
    }
  }

  public synchronized boolean hasLiveVMs() {
    for (var process : processes.values()) {
      if (process != null && process.isAlive()) {
        return true;
      }
    }
    return false;
  }

  private void linkStreams(final String version, final int vmNum, final ProcessHolder holder,
      final InputStream in, final PrintStream out) {
    final var vmName = "[" + VM.getVMName(version, vmNum) + "] ";
    var ioTransport = new Thread() {
      @Override
      public void run() {
        var reader = new BufferedReader(new InputStreamReader(in));
        try {
          var line = reader.readLine();
          while (line != null) {
            if (line.length() == 0) {
              out.println();
            } else {
              out.print(vmName);
              out.println(line);
            }
            line = reader.readLine();
          }
        } catch (Exception e) {
          if (!holder.isKilled()) {
            out.println("Error transporting IO from child process");
            e.printStackTrace(out);
          }
        }
      }
    };

    ioTransport.setDaemon(true);
    ioTransport.start();
  }

  private String removeModulesFromPath(String classpath, String... modules) {
    for (var module : modules) {
      classpath = removeModuleFromGradlePath(classpath, module);
      classpath = removeModuleFromEclipsePath(classpath, module);
      classpath = removeModuleFromIntelliJPath(classpath, module);
    }
    return classpath;
  }

  private String removeModuleFromEclipsePath(String classpath, String module) {
    var buildDir = File.separator + module + File.separator + "out" + File.separator;

    var mainClasses = buildDir + "production";
    if (!classpath.contains(mainClasses)) {
      return classpath;
    }

    classpath = removeFromPath(classpath, mainClasses);
    return classpath;
  }

  private String removeModuleFromIntelliJPath(String p_classpath, String module) {
    var classpath = p_classpath;
    var mainClasses = File.separator + "out" + File.separator + "production"
        + File.separator + "org.apache.geode." + module + ".main";
    if (classpath.contains(mainClasses)) {
      classpath = removeFromPath(classpath, mainClasses);
    }
    mainClasses = File.separator + "out" + File.separator + "production"
        + File.separator + "geode." + module + ".main";
    if (classpath.contains(mainClasses)) {
      classpath = removeFromPath(classpath, mainClasses);
    }
    return classpath;
  }

  private String removeModuleFromGradlePath(String classpath, String module) {
    var buildDir = File.separator + module + File.separator + "build" + File.separator;

    var mainClasses = buildDir + "classes" + File.separator + "java" + File.separator + "main";
    classpath = removeFromPath(classpath, mainClasses);

    var libDir = buildDir + "libs";
    classpath = removeFromPath(classpath, libDir);

    var mainResources = buildDir + "resources" + File.separator + "main";
    classpath = removeFromPath(classpath, mainResources);

    var generatedResources = buildDir + "generated-resources" + File.separator + "main";
    classpath = removeFromPath(classpath, generatedResources);

    return classpath;
  }

  private String[] buildJavaCommand(int vmNum, int namingPort, String version, int remoteStubPort)
      throws IOException {
    var cmd = System.getProperty("java.home") + File.separator
        + "bin" + File.separator + "java";
    String classPath;
    var dunitClasspath = System.getProperty("java.class.path");
    var separator = File.separator;
    if (VersionManager.isCurrentVersion(version)) {
      classPath = dunitClasspath;
    } else {
      // remove current-version product classes and resources from the classpath
      dunitClasspath =
          dunitClasspath =
              removeModulesFromPath(dunitClasspath, "geode-common", "geode-core", "geode-cq",
                  "geode-http-service", "geode-json", "geode-log4j", "geode-lucene",
                  "geode-serialization", "geode-wan", "geode-gfsh");
      classPath = versionManager.getClasspath(version) + File.pathSeparator + dunitClasspath;
    }
    var jreLib = separator + "jre" + separator + "lib" + separator;
    classPath = removeFromPath(classPath, jreLib);

    // String tmpDir = System.getProperty("java.io.tmpdir");
    var agent = getAgentString();

    var jdkDebug = "";
    if (debugPort > 0) {
      jdkDebug += ",address=" + debugPort;
      debugPort++;
    }

    var jdkSuspend = vmNum == suspendVM ? "y" : "n"; // ignore version
    var cmds = new ArrayList<String>();
    cmds.add(cmd);
    cmds.add("-classpath");
    // Set the classpath via a "pathing" jar to shorten cmd to a length allowed by Windows.
    cmds.add(createPathingJar(getVMDir(version, vmNum).getName(), classPath).toString());
    cmds.add("-D" + DUnitLauncher.REMOTE_STUB_PORT_PARAM + "=" + remoteStubPort);
    cmds.add("-D" + DUnitLauncher.RMI_PORT_PARAM + "=" + namingPort);
    cmds.add("-D" + DUnitLauncher.VM_NUM_PARAM + "=" + vmNum);
    cmds.add("-D" + DUnitLauncher.VM_VERSION_PARAM + "=" + version);
    cmds.add("-D" + DUnitLauncher.WORKSPACE_DIR_PARAM + "=" + new File(".").getAbsolutePath());
    cmds.add("-DAvailablePort.lowerBound=" + AvailablePort.AVAILABLE_PORTS_LOWER_BOUND);
    cmds.add("-DAvailablePort.upperBound=" + AvailablePort.AVAILABLE_PORTS_UPPER_BOUND);
    var membershipPortRange = System.getProperty(GEMFIRE_PREFIX + MEMBERSHIP_PORT_RANGE_NAME);
    if (membershipPortRange != null) {
      cmds.add(
          String.format("-D%s=%s", GEMFIRE_PREFIX + MEMBERSHIP_PORT_RANGE_NAME,
              membershipPortRange));
    }
    if (vmNum >= 0) { // let the locator print a banner
      if (version.equals(VersionManager.CURRENT_VERSION)) { // enable the banner for older versions
        cmds.add("-D" + InternalLocator.INHIBIT_DM_BANNER + "=true");
      }
    } else {
      // most distributed unit tests were written under the assumption that network partition
      // detection is disabled, so we turn it off in the locator. Tests for network partition
      // detection should create a separate locator that has it enabled
      cmds.add(
          "-D" + GEMFIRE_PREFIX + ENABLE_NETWORK_PARTITION_DETECTION + "=false");
      cmds.add(
          "-D" + GEMFIRE_PREFIX + "allow_old_members_to_join_for_testing=true");
    }
    if (DUnitLauncher.LOG4J != null) {
      cmds.add("-Dlog4j.configurationFile=" + DUnitLauncher.LOG4J);
    }
    cmds.add("-Djava.library.path=" + System.getProperty("java.library.path"));
    cmds.add("-Xrunjdwp:transport=dt_socket,server=y,suspend=" + jdkSuspend + jdkDebug);
    cmds.add("-XX:+HeapDumpOnOutOfMemoryError");
    cmds.add("-Xmx512m");
    cmds.add("-D" + GEMFIRE_PREFIX + "DEFAULT_MAX_OPLOG_SIZE=10");
    cmds.add("-D" + GEMFIRE_PREFIX + "disallowMcastDefaults=true");
    cmds.add("-D" + DistributionConfig.RESTRICT_MEMBERSHIP_PORT_RANGE + "=true");
    cmds.add("-D" + GEMFIRE_PREFIX
        + ConfigurationProperties.VALIDATE_SERIALIZABLE_OBJECTS + "=true");
    cmds.add("-ea");
    cmds.add("-XX:MetaspaceSize=512m");
    cmds.add("-XX:SoftRefLRUPolicyMSPerMB=1");
    cmds.add(agent);
    cmds.addAll(getJvmModuleOptions());
    cmds.add(ChildVM.class.getName());
    var rst = new String[cmds.size()];
    cmds.toArray(rst);

    return rst;
  }

  // Write the entire classpath to a jar file. The command line can specify the child VM's
  // classpath by naming only this single file, which shortens the command line to a length that
  // Windows allows.
  private Path createPathingJar(String vmName, String classPath) throws IOException {
    var currentWorkingDir = Paths.get("").toAbsolutePath();
    var pathingJarPath = currentWorkingDir.resolve(vmName + "-pathing.jar");

    var originalClassPathEntries = Arrays.asList(classPath.split(File.pathSeparator));
    var classPathAttributeValue = originalClassPathEntries.stream()
        .map(Paths::get)
        .filter(Files::exists)
        .map(currentWorkingDir::relativize) // Entries must be relative to pathing jar's dir
        .map(p -> Files.isDirectory(p) ? p + "/" : p.toString()) // Dir entries must end with /
        .map(s -> s.replaceAll("\\\\", "/")) // Separator must be /
        .collect(joining(" "));

    var manifest = new Manifest();
    var mainAttributes = manifest.getMainAttributes();
    mainAttributes.putValue(Attributes.Name.MANIFEST_VERSION.toString(), "1.0");
    mainAttributes.putValue(Attributes.Name.CLASS_PATH.toString(), classPathAttributeValue);

    var fileOutputStream = new FileOutputStream(pathingJarPath.toFile());
    var jarOutputStreamStream = new JarOutputStream(fileOutputStream, manifest);
    jarOutputStreamStream.close();
    return pathingJarPath;
  }

  private String removeFromPath(String classpath, String partialPath) {
    var jars = classpath.split(File.pathSeparator);
    var sb = new StringBuilder(classpath.length());
    Boolean firstjar = true;
    for (var jar : jars) {
      if (!jar.contains(partialPath)) {
        if (!firstjar) {
          sb.append(File.pathSeparator);
        }
        sb.append(jar);
        firstjar = false;
      }
    }
    return sb.toString();
  }

  /**
   * Get the java agent passed to this process and pass it to the child VMs. This was added to
   * support jacoco code coverage reports
   */
  private String getAgentString() {
    var runtimeBean = ManagementFactory.getRuntimeMXBean();
    if (runtimeBean != null) {
      for (var arg : runtimeBean.getInputArguments()) {
        if (arg.contains("-javaagent:")) {
          // HACK for gradle bug GRADLE-2859. Jacoco is passing a relative path
          // That won't work when we pass this to dunit VMs in a different
          // directory
          arg = arg.replace("-javaagent:..",
              "-javaagent:" + System.getProperty("user.dir") + File.separator + "..");
          arg = arg.replace("destfile=..",
              "destfile=" + System.getProperty("user.dir") + File.separator + "..");
          return arg;
        }
      }
    }

    return "-DdummyArg=true";
  }

  synchronized void signalVMReady() {
    pendingVMs--;
    notifyAll();
  }

  public synchronized boolean waitForVMs() throws InterruptedException {
    return waitForVMs(DUnitLauncher.STARTUP_TIMEOUT);
  }

  public synchronized boolean waitForVMs(long timeout) throws InterruptedException {
    var end = System.currentTimeMillis() + timeout;
    while (pendingVMs > 0) {
      var remaining = end - System.currentTimeMillis();
      if (remaining <= 0) {
        return false;
      }
      wait(remaining);
    }

    return true;
  }

  @Override
  public RemoteDUnitVMIF getStub(int i)
      throws RemoteException, NotBoundException, InterruptedException {
    return getStub(VersionManager.CURRENT_VERSION, i);
  }

  public RemoteDUnitVMIF getStub(String version, int i)
      throws RemoteException, NotBoundException, InterruptedException {
    waitForVMs(DUnitLauncher.STARTUP_TIMEOUT);
    return (RemoteDUnitVMIF) registry.lookup("vm" + i);
  }

  public ProcessHolder getProcessHolder(int i) {
    return processes.get(i);
  }
}
