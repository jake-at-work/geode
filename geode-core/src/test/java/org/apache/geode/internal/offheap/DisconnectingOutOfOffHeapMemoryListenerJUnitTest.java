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

package org.apache.geode.internal.offheap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import org.apache.geode.LogWriter;
import org.apache.geode.OutOfOffHeapMemoryException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;

public class DisconnectingOutOfOffHeapMemoryListenerJUnitTest {

  private final InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
  private final OutOfOffHeapMemoryException ex = new OutOfOffHeapMemoryException();
  private final LogWriter lw = mock(LogWriter.class);
  private final DistributionManager dm = mock(DistributionManager.class);

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    when(ids.getLogWriter()).thenReturn(lw);
    when(ids.getDistributionManager()).thenReturn(dm);
  }

  @Test
  public void constructWithNullSupportsClose() {
    var listener =
        new DisconnectingOutOfOffHeapMemoryListener(null);
    listener.close();
  }

  @Test
  public void constructWithNullSupportsOutOfOffHeapMemory() {
    var listener =
        new DisconnectingOutOfOffHeapMemoryListener(null);
    listener.outOfOffHeapMemory(null);
  }

  @Test
  public void disconnectNotCalledWhenSysPropIsSet() {
    System.setProperty(OffHeapStorage.STAY_CONNECTED_ON_OUTOFOFFHEAPMEMORY_PROPERTY, "true");
    var listener =
        new DisconnectingOutOfOffHeapMemoryListener(ids);
    listener.outOfOffHeapMemory(ex);
    verify(ids, never()).disconnect(ex.getMessage(), false);
  }

  @Test
  public void disconnectNotCalledWhenListenerClosed() {
    var listener =
        new DisconnectingOutOfOffHeapMemoryListener(ids);
    listener.close();
    listener.outOfOffHeapMemory(ex);
    verify(ids, never()).disconnect(ex.getMessage(), false);
  }

  @Test
  public void setRootCauseCalledWhenGetRootCauseReturnsNull() {
    var listener =
        new DisconnectingOutOfOffHeapMemoryListener(ids);
    when(dm.getRootCause()).thenReturn(null);
    listener.outOfOffHeapMemory(ex);
    verify(dm).setRootCause(ex);
  }

  @Test
  public void setRootCauseNotCalledWhenGetRootCauseReturnsNonNull() {
    var listener =
        new DisconnectingOutOfOffHeapMemoryListener(ids);
    when(dm.getRootCause()).thenReturn(ex);
    listener.outOfOffHeapMemory(ex);
    verify(dm, never()).setRootCause(ex);
  }

  @Test
  public void disconnectCalledAsyncAfterCallingOutOfOffHeapMemory() {
    var listener =
        new DisconnectingOutOfOffHeapMemoryListener(ids);
    listener.outOfOffHeapMemory(ex);
    verify(ids, timeout(5000).atLeastOnce()).disconnect(ex.getMessage(), false);
    verify(lw).info("OffHeapStorage about to invoke disconnect on " + ids);
  }

}
