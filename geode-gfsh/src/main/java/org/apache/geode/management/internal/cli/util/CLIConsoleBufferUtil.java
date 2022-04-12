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
package org.apache.geode.management.internal.cli.util;


import org.apache.geode.management.internal.cli.shell.Gfsh;

public class CLIConsoleBufferUtil {
  public static String processMessegeForExtraCharactersFromConsoleBuffer(String messege) {

    var reader = Gfsh.getConsoleReader();
    if (reader != null) {
      var bufferLength = reader.getCursorBuffer().length();
      if (bufferLength > messege.length()) {
        var appendSpaces = bufferLength - messege.length();
        for (var i = 0; i < appendSpaces; i++) {
          messege = messege + " ";
        }
      }
    }
    return messege;
  }
}
