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
package org.apache.geode.internal.statistics;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.internal.SystemAdmin.StatSpec;
import org.apache.geode.internal.statistics.StatArchiveReader.ResourceInst;

/**
 * @since Geode 1.0
 */
public class StatUtils {

  /**
   * final File expectedStatArchiveFile = new File(TestUtil.getResourcePath(getClass(),
   * "StatArchiveWriterReaderJUnitTest_" + this.testName.getMethodName() + "_expected.gfs"));
   */
  public static void compareStatArchiveFiles(final File expectedStatArchiveFile,
      final File actualStatArchiveFile) throws IOException {
    assertThat(expectedStatArchiveFile).exists();
    assertThat(actualStatArchiveFile.length()).isEqualTo(expectedStatArchiveFile.length());

    assertThat(readBytes(actualStatArchiveFile)).isEqualTo(readBytes(expectedStatArchiveFile));
  }

  public static Set<ResourceInst> findResourceInsts(final File archiveFile, final String specString)
      throws IOException {
    Set<ResourceInst> resourceInsts = new HashSet<>();

    if (StringUtils.isNotEmpty(specString)) {
      addResourceInstsToSet(archiveFile, specString, resourceInsts);
    } else {
      addResourceInstsToSet(archiveFile, resourceInsts);
    }

    return resourceInsts;
  }

  private static void addResourceInstsToSet(final File archiveFile,
      final Set<ResourceInst> resourceInsts) throws IOException {
    var reader =
        new StatArchiveReader(new File[] {archiveFile}, new StatSpec[] {}, true);

    for (final var o : (Iterable<ResourceInst>) reader.getResourceInstList()) {
      resourceInsts.add(o);
    }
  }

  private static void addResourceInstsToSet(final File archiveFile, final String specString,
      final Set<ResourceInst> resourceInsts) throws IOException {
    var statSpec = new StatSpec(specString);

    var reader =
        new StatArchiveReader(new File[] {archiveFile}, new StatSpec[] {statSpec}, true);
    var statValues = reader.matchSpec(statSpec);

    for (var statValue : statValues) {
      for (var resourceInst : statValue.getResources()) {
        resourceInsts.add(resourceInst);
      }
    }
  }

  private static byte[] readBytes(File file) throws IOException {
    var byteCount = (int) file.length();

    var input = new byte[byteCount];

    var url = file.toURL();
    assertThat(url).isNotNull();

    var is = url.openStream();
    assertThat(is).isNotNull();

    var bis = new BufferedInputStream(is);
    var bytesRead = bis.read(input);
    bis.close();

    assertThat(bytesRead).isEqualTo(byteCount);
    return input;
  }
}
