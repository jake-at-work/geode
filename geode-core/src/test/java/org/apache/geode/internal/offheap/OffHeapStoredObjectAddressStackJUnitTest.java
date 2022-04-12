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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

import org.apache.logging.log4j.Logger;
import org.junit.Test;


public class OffHeapStoredObjectAddressStackJUnitTest {

  @Test
  public void addressZeroCausesStackToBeEmpty() {
    var stack = new OffHeapStoredObjectAddressStack(0L);
    assertEquals(true, stack.isEmpty());
  }

  @Test
  public void defaultStackIsEmpty() {
    var stack = new OffHeapStoredObjectAddressStack();
    assertEquals(true, stack.isEmpty());
  }

  @Test
  public void defaultStackReturnsZeroFromTop() {
    var stack = new OffHeapStoredObjectAddressStack();
    assertEquals(0L, stack.getTopAddress());
  }

  @Test
  public void defaultStackReturnsZeroFromPoll() {
    var stack = new OffHeapStoredObjectAddressStack();
    assertEquals(0L, stack.poll());
  }

  @Test
  public void defaultStackReturnsZeroFromClear() {
    var stack = new OffHeapStoredObjectAddressStack();
    assertEquals(0L, stack.clear());
    assertEquals(true, stack.isEmpty());
  }

  @Test
  public void defaultStackLogsNothing() {
    var stack = new OffHeapStoredObjectAddressStack();
    var lw = mock(Logger.class, withSettings().invocationListeners(
        methodInvocationReport -> fail("Unexpected invocation")));
    stack.logSizes(lw, "should not be used");
  }

  @Test
  public void defaultStackComputeSizeIsZero() {
    var stack = new OffHeapStoredObjectAddressStack();
    assertEquals(0L, stack.computeTotalSize());
  }

  @Test
  public void stackCreatedWithAddressIsNotEmpty() {
    var slab = new SlabImpl(1024);
    try {
      var ma =
          MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
              new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
      var chunk = (OffHeapStoredObject) ma.allocate(100);

      var stack =
          new OffHeapStoredObjectAddressStack(chunk.getAddress());
      assertEquals(false, stack.isEmpty());
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  @Test
  public void stackWithChunkIsNotEmpty() {
    var slab = new SlabImpl(1024);
    try {
      var ma =
          MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
              new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
      var chunk = (OffHeapStoredObject) ma.allocate(100);

      var stack = new OffHeapStoredObjectAddressStack();
      stack.offer(chunk.getAddress());
      assertEquals(false, stack.isEmpty());
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  @Test
  public void stackWithChunkTopEqualsAddress() {
    var slab = new SlabImpl(1024);
    try {
      var ma =
          MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
              new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
      var chunk = (OffHeapStoredObject) ma.allocate(100);

      var addr = chunk.getAddress();
      var stack = new OffHeapStoredObjectAddressStack();
      stack.offer(addr);
      assertEquals(addr, stack.getTopAddress());
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  @Test
  public void addressZeroOfferCausesFailedAssertion() {
    var stack = new OffHeapStoredObjectAddressStack(0L);
    try {
      stack.offer(0);
      fail("expected AssertionError");
    } catch (AssertionError expected) {
    }
  }


  @Test
  public void stackWithChunkClearReturnsAddressAndEmptiesStack() {
    var slab = new SlabImpl(1024);
    try {
      var ma =
          MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
              new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
      var chunk = (OffHeapStoredObject) ma.allocate(100);

      var addr = chunk.getAddress();
      var stack = new OffHeapStoredObjectAddressStack();
      stack.offer(addr);
      var clearAddr = stack.clear();
      assertEquals(addr, clearAddr);
      assertEquals(true, stack.isEmpty());
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  @Test
  public void stackWithChunkPollReturnsAddressAndEmptiesStack() {
    var slab = new SlabImpl(1024);
    try {
      var ma =
          MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
              new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
      var chunk = (OffHeapStoredObject) ma.allocate(100);

      var addr = chunk.getAddress();
      var stack = new OffHeapStoredObjectAddressStack();
      stack.offer(addr);
      var pollAddr = stack.poll();
      assertEquals(addr, pollAddr);
      assertEquals(true, stack.isEmpty());
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  @Test
  public void stackWithChunkTotalSizeIsChunkSize() {
    var slab = new SlabImpl(1024);
    try {
      var ma =
          MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
              new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
      var chunk = (OffHeapStoredObject) ma.allocate(100);
      var chunkSize = chunk.getSize();

      var addr = chunk.getAddress();
      var stack = new OffHeapStoredObjectAddressStack();
      stack.offer(addr);
      assertEquals(chunkSize, stack.computeTotalSize());
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }


  @Test
  public void stackWithChunkLogShowsMsgAndSize() {
    var slab = new SlabImpl(1024);
    try {
      var ma =
          MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
              new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
      var chunk = (OffHeapStoredObject) ma.allocate(100);
      var chunkSize = chunk.getSize();

      var addr = chunk.getAddress();
      var stack = new OffHeapStoredObjectAddressStack();
      stack.offer(addr);
      var lw = mock(Logger.class);
      stack.logSizes(lw, "foo");
      verify(lw).info("foo" + chunkSize);
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  private class TestableSyncChunkStack extends OffHeapStoredObjectAddressStack {
    public boolean doConcurrentMod = true;
    public int chunk2Size;
    private final MemoryAllocatorImpl ma;

    TestableSyncChunkStack(MemoryAllocatorImpl ma) {
      this.ma = ma;
    }

    @Override
    protected void testHookDoConcurrentModification() {
      if (doConcurrentMod) {
        doConcurrentMod = false;
        var chunk2 = (OffHeapStoredObject) ma.allocate(50);
        chunk2Size = chunk2.getSize();
        offer(chunk2.getAddress());
      }
    }
  }

  @Test
  public void stackWithChunkTotalSizeIsChunkSizeWithConcurrentMod() {
    var slab = new SlabImpl(1024);
    try {
      var ma =
          MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
              new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
      var chunk = (OffHeapStoredObject) ma.allocate(100);
      var chunkSize = chunk.getSize();

      var addr = chunk.getAddress();
      var stack = new TestableSyncChunkStack(ma);
      stack.offer(addr);
      var totalSize = stack.computeTotalSize();
      assertEquals("chunkSize=" + chunkSize + " chunk2Size=" + stack.chunk2Size,
          chunkSize + stack.chunk2Size, totalSize);
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }


  @Test
  public void stackWithChunkLogShowsMsgAndSizeWithConcurrentMod() {
    var slab = new SlabImpl(1024);
    try {
      var ma =
          MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
              new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
      var chunk = (OffHeapStoredObject) ma.allocate(100);
      var chunkSize = chunk.getSize();

      var addr = chunk.getAddress();
      var stack = new TestableSyncChunkStack(ma);
      stack.offer(addr);
      var lw = mock(Logger.class);
      stack.logSizes(lw, "foo");
      verify(lw).info("foo" + chunkSize);
      verify(lw).info("foo" + stack.chunk2Size);
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }
}
