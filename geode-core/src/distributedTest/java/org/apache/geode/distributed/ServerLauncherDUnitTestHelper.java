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

package org.apache.geode.distributed;

public class ServerLauncherDUnitTestHelper {
  public static void main(String[] args) {

    var locatorPort = Integer.parseInt(args[0]);

    System.setProperty("gemfire.disableShutdownHook", "true");

    final var serverLauncher =
        new ServerLauncher.Builder().setCommand(ServerLauncher.Command.START)
            .setMemberName("server1")
            .set("locators", "localhost[" + locatorPort + "]")
            .set("log-level", "config")
            .set("log-file", "")
            .setServerPort(0)
            .setDebug(true)
            .build();

    serverLauncher.start();

    var t = new Thread(serverLauncher::stop);

    t.setDaemon(true);
    t.start();
  }
}
