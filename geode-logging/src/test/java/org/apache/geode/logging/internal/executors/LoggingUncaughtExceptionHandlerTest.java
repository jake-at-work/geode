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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.logging.internal.executors.LoggingUncaughtExceptionHandler.FailureSetter;
import org.apache.geode.logging.internal.executors.LoggingUncaughtExceptionHandler.Implementation;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Unit tests for {@link
 * org.apache.geode.logging.internal.executors.LoggingUncaughtExceptionHandler}.
 */
@Category(LoggingTest.class)
public class LoggingUncaughtExceptionHandlerTest {

  @Test
  public void verifyGetInstanceIsNotNull() {
    var handler = LoggingUncaughtExceptionHandler.getInstance();

    assertThat(handler).isNotNull();
  }

  @Test
  public void verifyThatSetOnThreadSetsTheThreadsHandler() {
    var thread = new Thread();
    var handler = new Implementation(null);

    handler.setOnThread(thread);

    assertThat(thread.getUncaughtExceptionHandler()).isSameAs(handler);
  }

  @Test
  public void verifyThatCallingUncaughtExceptionIncreasesTheCountByOne() {
    var logger = mock(Logger.class);
    var handler = new Implementation(logger);
    var count = handler.getUncaughtExceptionsCount();

    handler.uncaughtException(null, null);

    assertThat(handler.getUncaughtExceptionsCount()).isEqualTo(count + 1);
  }

  @Test
  public void verifyThatCallingClearSetsTheCountToZero() {
    var logger = mock(Logger.class);
    var handler = new Implementation(logger);
    // force the count to be non-zero
    handler.uncaughtException(null, null);

    handler.clearUncaughtExceptionsCount();

    assertThat(handler.getUncaughtExceptionsCount()).isEqualTo(0);
  }

  @Test
  public void verifyFatalMessageLoggedWhenUncaughtExceptionIsCalled() {
    var logger = mock(Logger.class);
    var thread = mock(Thread.class);
    var throwable = mock(Throwable.class);
    var handler = new Implementation(logger);

    handler.uncaughtException(thread, throwable);

    verify(logger).fatal("Uncaught exception in thread " + thread, throwable);
  }

  @Test
  public void verifyInfoMessageLoggedWhenUncaughtExceptionIsCalledWithTreatExceptionAsFatalFalse() {
    var logger = mock(Logger.class);
    Thread thread = new LoggingThread("test", false, () -> {
    }, false);
    Throwable throwable = mock(NoClassDefFoundError.class);
    var handler = new Implementation(logger);

    handler.uncaughtException(thread, throwable);

    verify(logger).info(
        "Uncaught exception in thread {} this message can be disregarded if it occurred during an Application Server shutdown. The Exception message was: {}",
        thread, throwable);
  }

  @Test
  public void verifySetFailureCalledWhenUncaughtExceptionCalledWithVirtualMachineError() {
    var logger = mock(Logger.class);
    var thread = mock(Thread.class);
    var error = mock(VirtualMachineError.class);
    var failureSetter = mock(FailureSetter.class);
    var handler = new Implementation(logger);
    handler.setFailureSetter(failureSetter);
    handler.uncaughtException(thread, error);

    verify(failureSetter).setFailure(error);
  }

}
