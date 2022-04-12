/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.logging.internal.executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.logging.internal.executors.LoggingThreadFactory.CommandWrapper;
import org.apache.geode.logging.internal.executors.LoggingThreadFactory.ThreadInitializer;
import org.apache.geode.test.junit.categories.LoggingTest;

/** Unit tests for {@link org.apache.geode.logging.internal.executors.LoggingThreadFactory}. */
@Category(LoggingTest.class)
public class LoggingThreadFactoryTest {

  @Test
  public void verifyFirstThreadName() {
    var factory = new LoggingThreadFactory("baseName");

    var thread = factory.newThread(null);

    assertThat(thread.getName()).isEqualTo("baseName" + 1);
  }

  @Test
  public void verifySecondThreadName() {
    var factory = new LoggingThreadFactory("baseName");
    factory.newThread(null);

    var thread = factory.newThread(null);

    assertThat(thread.getName()).isEqualTo("baseName" + 2);
  }

  @Test
  public void verifyThreadsAreDaemons() {
    var factory = new LoggingThreadFactory("baseName");

    var thread = factory.newThread(null);

    assertThat(thread.isDaemon()).isTrue();
  }

  @Test
  public void verifyThreadHaveExpectedHandler() {
    var handler = LoggingUncaughtExceptionHandler.getInstance();
    var factory = new LoggingThreadFactory("baseName");

    var thread = factory.newThread(null);

    assertThat(thread.getUncaughtExceptionHandler()).isSameAs(handler);
  }

  @Test
  public void verifyThreadInitializerCalledCorrectly() {
    var threadInitializer = mock(ThreadInitializer.class);
    var factory = new LoggingThreadFactory("baseName", threadInitializer, null);

    var thread = factory.newThread(null);

    verify(threadInitializer).initialize(thread);
  }

  @Test
  public void verifyCommandWrapperNotCalledIfThreadIsNotStarted() {
    var commandWrapper = mock(CommandWrapper.class);
    var factory = new LoggingThreadFactory("baseName", commandWrapper);

    var thread = factory.newThread(null);

    verify(commandWrapper, never()).invoke(any());
  }

  @Test
  public void verifyCommandWrapperCalledIfThreadStarted() throws InterruptedException {
    var commandWrapper = mock(CommandWrapper.class);
    var command = mock(Runnable.class);
    var factory = new LoggingThreadFactory("baseName", commandWrapper);

    var thread = factory.newThread(command);
    thread.start();
    thread.join();

    verify(commandWrapper).invoke(command);
  }

  @Test
  public void verifyCommandCalledIfThreadStarted() throws InterruptedException {
    var command = mock(Runnable.class);
    var factory = new LoggingThreadFactory("baseName");

    var thread = factory.newThread(command);
    thread.start();
    thread.join();

    verify(command).run();
  }
}
