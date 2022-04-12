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
package org.apache.geode.management.internal.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.management.internal.configuration.utils.ZipUtils;

/**
 * JUnit Test for {@link ZipUtils}
 */
public class ZipUtilsJUnitTest {

  private final String destinationFolderName = "destination";
  private final String clusterFolderName = "cluster";
  private final String groupFolderName = "group";
  private final String clusterTextFileName = "cf.txt";
  private final String groupTextFileName = "gf.txt";
  private final String clusterText = "cluster content";
  private final String groupText = "group content";

  private File sourceFolder;
  private File zipFolder;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    sourceFolder = temporaryFolder.newFolder("sourceFolder");

    var clusterFolder = new File(sourceFolder.getCanonicalPath(), clusterFolderName);
    assertTrue(clusterFolder.mkdir());

    var groupFolder = new File(sourceFolder.getCanonicalPath(), groupFolderName);
    assertTrue(groupFolder.mkdir());

    zipFolder = temporaryFolder.newFolder("zipFolder");

    FileUtils.writeStringToFile(
        new File(FilenameUtils.concat(clusterFolder.getCanonicalPath(), clusterTextFileName)),
        clusterText);
    FileUtils.writeStringToFile(
        new File(FilenameUtils.concat(groupFolder.getCanonicalPath(), groupTextFileName)),
        groupText);
  }

  @Test
  public void testZipUtils() throws Exception {
    var zipFile = new File(zipFolder, "target.zip");
    assertFalse(zipFile.exists());
    assertFalse(zipFile.isFile());

    ZipUtils.zipDirectory(sourceFolder.getCanonicalPath(), zipFile.getCanonicalPath());
    assertTrue(zipFile.exists());
    assertTrue(zipFile.isFile());

    var destinationFolder = new File(
        FilenameUtils.concat(temporaryFolder.getRoot().getCanonicalPath(), destinationFolderName));
    assertFalse(destinationFolder.exists());
    assertFalse(destinationFolder.isFile());

    ZipUtils.unzip(zipFile.getCanonicalPath(), destinationFolder.getCanonicalPath());
    assertTrue(destinationFolder.exists());
    assertTrue(destinationFolder.isDirectory());

    var destinationSubDirs = destinationFolder.listFiles();
    assertNotNull(destinationSubDirs);
    assertEquals(2, destinationSubDirs.length);

    var destinationClusterTextFile =
        new File(FilenameUtils.concat(destinationFolder.getCanonicalPath(),
            clusterFolderName + File.separator + clusterTextFileName));
    assertTrue(destinationClusterTextFile.exists());
    assertTrue(destinationClusterTextFile.isFile());

    var destinationGroupTextFile =
        new File(FilenameUtils.concat(destinationFolder.getCanonicalPath(),
            groupFolderName + File.separator + groupTextFileName));
    assertTrue(destinationGroupTextFile.exists());
    assertTrue(destinationGroupTextFile.isFile());

    assertTrue(clusterText.equals(FileUtils.readFileToString(destinationClusterTextFile)));
    assertTrue(groupText.equals(FileUtils.readFileToString(destinationGroupTextFile)));
  }

  @Test
  public void zipUtilsCanCreateParentDirsIfNecessary() throws IOException {
    var newFolder = new File(zipFolder, "newFolder");
    assertFalse(newFolder.exists());

    var zipFile = new File(newFolder, "target.zip");
    assertFalse(zipFile.exists());
    assertFalse(zipFile.isFile());

    ZipUtils.zipDirectory(sourceFolder.getCanonicalPath(), zipFile.getCanonicalPath());
    assertTrue(newFolder.exists());
    assertTrue(zipFile.exists());
    assertTrue(zipFile.isFile());
  }
}
