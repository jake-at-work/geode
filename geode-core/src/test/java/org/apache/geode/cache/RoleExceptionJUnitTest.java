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
package org.apache.geode.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.distributed.internal.membership.InternalRole;

/**
 * Tests the subclasses of RoleException to make sure they are Serializable
 */
public class RoleExceptionJUnitTest {

  /**
   * Assert that RegionAccessException is serializable.
   */
  @Test
  public void testRegionAccessExceptionIsSerializable() throws Exception {
    var out = createRegionAccessException();
    var baos = new ByteArrayOutputStream(100);
    var oos = new ObjectOutputStream(baos);
    oos.writeObject(out);

    var data = baos.toByteArray();

    var bais = new ByteArrayInputStream(data);
    var ois = new ObjectInputStream(bais);
    var in = (RegionAccessException) ois.readObject();
    assertEquals(createSetOfRoles(), in.getMissingRoles());

    assertEquals(out.getMessage(), in.getMessage());
    assertEquals(out.getRegionFullPath(), in.getRegionFullPath());
  }

  /**
   * Assert that RegionDistributionException is serializable.
   */
  @Test
  public void testRegionDistributionExceptionIsSerializable() throws Exception {
    var out = createRegionDistributionException();
    var baos = new ByteArrayOutputStream(100);
    var oos = new ObjectOutputStream(baos);
    oos.writeObject(out);

    var data = baos.toByteArray();

    var bais = new ByteArrayInputStream(data);
    var ois = new ObjectInputStream(bais);
    var in = (RegionDistributionException) ois.readObject();
    assertEquals(createSetOfRoles(), in.getFailedRoles());

    assertEquals(out.getMessage(), in.getMessage());
    assertEquals(out.getRegionFullPath(), in.getRegionFullPath());
  }

  /**
   * Assert that CommitDistributionException is serializable.
   */
  @Test
  public void testCommitDistributionExceptionIsSerializable() throws Exception {
    var s = "MyString";
    Set outExceptions = new HashSet();
    outExceptions.add(createRegionDistributionException());

    var out = new CommitDistributionException(s, outExceptions);
    var baos = new ByteArrayOutputStream(100);
    var oos = new ObjectOutputStream(baos);
    oos.writeObject(out);

    var data = baos.toByteArray();

    var bais = new ByteArrayInputStream(data);
    var ois = new ObjectInputStream(bais);
    var in = (CommitDistributionException) ois.readObject();

    var inExceptions = in.getRegionDistributionExceptions();
    assertNotNull(inExceptions);
    var iter = inExceptions.iterator();
    assertTrue(iter.hasNext());
    var e = (RegionDistributionException) iter.next();
    assertEquals(createSetOfRoles(), e.getFailedRoles());

    assertEquals(out.getMessage(), in.getMessage());
  }

  private Set createSetOfRoles() {
    Set set = new HashSet();
    set.add(InternalRole.getRole("RoleA"));
    set.add(InternalRole.getRole("RoleB"));
    set.add(InternalRole.getRole("RoleC"));
    set.add(InternalRole.getRole("RoleD"));
    return set;
  }

  private RegionAccessException createRegionAccessException() {
    var s = "MyString";
    var regionFullPath = "MyPath";
    var missingRoles = createSetOfRoles();
    return new RegionAccessException(s, regionFullPath, missingRoles);
  }

  private RegionDistributionException createRegionDistributionException() {
    var s = "MyString";
    var regionFullPath = "MyPath";
    var missingRoles = createSetOfRoles();
    return new RegionDistributionException(s, regionFullPath, missingRoles);
  }

}
