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
package org.apache.geode.sequence;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;


public class HydraLineMapper implements LineMapper {
  private static final Pattern VM_NAME_PATTERN = Pattern.compile("(vm_\\d+).*_(\\d+)(_end)?\\.log");
  private static final Pattern DISK_DIR_PATTERN = Pattern.compile("vm_(\\d+).*_disk_1");
  private final Map<String, String> processIdToVMName = new HashMap<>();
  private final DefaultLineMapper defaultMapper = new DefaultLineMapper();

  public HydraLineMapper(File[] graphFiles) {
    var firstFile = graphFiles[0];
    var directory = firstFile.getParentFile();
    if (directory == null || !new File(directory, "latest.prop").exists()) {
      directory = new File(".");
    }
    var files = directory.list();
    for (var file : files) {
      var matcher = VM_NAME_PATTERN.matcher(file);
      if (matcher.matches()) {
        processIdToVMName.put(matcher.group(2), matcher.group(1));
      }
    }

    for (var file : files) {
      var matcher = DISK_DIR_PATTERN.matcher(file);
      if (matcher.matches()) {

        var storeId = getDiskStoreId(file);
        if (storeId != null) {
          processIdToVMName.put(storeId, "disk_" + matcher.group(1));
        }
      }
    }


  }

  private String getDiskStoreId(String diskStoreDir) {
    var dir = new File(diskStoreDir);
    var files = dir.list();
    for (var fileName : files) {
      if (fileName.endsWith(".if")) {
        try {
          return getDiskStoreIdFromInitFile(dir, fileName);
        } catch (Exception e) {
          return null;
        }
      }
    }

    return null;
  }

  private String getDiskStoreIdFromInitFile(File dir, String fileName)
      throws IOException {
    var fis = new FileInputStream(new File(dir, fileName));
    try {
      var bytes = new byte[1 + 8 + 8];
      fis.read(bytes);
      var buffer = ByteBuffer.wrap(bytes);
      // Skip the record type.
      buffer.get();
      var least = buffer.getLong();
      var most = buffer.getLong();
      var id = new UUID(most, least);
      return id.toString();
    } finally {
      fis.close();
    }
  }

  @Override
  public String getShortNameForLine(String lineName) {
    var name = defaultMapper.getShortNameForLine(lineName);
    if (processIdToVMName.containsKey(name)) {
      return processIdToVMName.get(name);
    } else {
      return name;
    }
  }

  public static boolean isInHydraRun(File[] graphFiles) {
    if (graphFiles.length == 0) {
      return false;
    }
    var firstFile = graphFiles[0];
    var parentFile = firstFile.getParentFile();
    for (var file : graphFiles) {
      if (parentFile == null && file.getParentFile() == null) {
        return true;
      }
      if (parentFile == null || file.getParentFile() == null
          || !file.getParentFile().equals(parentFile)) {
        return false;
      }
    }

    return new File(parentFile, "latest.prop").exists() || new File("latest.prop").exists();


  }

}
