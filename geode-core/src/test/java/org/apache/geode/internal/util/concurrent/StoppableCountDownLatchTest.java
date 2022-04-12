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
package org.apache.geode.internal.util.concurrent;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.geode.internal.util.concurrent.StoppableCountDownLatch.RETRY_TIME_MILLIS_DEFAULT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.ErrorCollector;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class StoppableCountDownLatchTest {

  private static final long TIMEOUT_MILLIS = getTimeout().toMillis();

  private CancelCriterion stopper;

  @Rule
  public ErrorCollector errorCollector = new ErrorCollector();

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() {
    stopper = mock(CancelCriterion.class);
  }

  @Test
  public void defaultRetryIntervalNanosIsTwoSeconds() {
    long twoSeconds = 2;
    var latch = new StoppableCountDownLatch(stopper, 1);

    assertThat(NANOSECONDS.toSeconds(latch.retryIntervalNanos()))
        .isEqualTo(MILLISECONDS.toSeconds(RETRY_TIME_MILLIS_DEFAULT))
        .isEqualTo(twoSeconds);
  }

  @Test
  public void awaitReturnsAfterCountDown() {
    var latch =
        new StoppableCountDownLatch(stopper, 1, MILLISECONDS.toNanos(2), System::nanoTime);

    var latchFuture = executorServiceRule.submit(() -> latch.await());

    latch.countDown();

    await().untilAsserted(() -> assertThat(latchFuture.isDone()).isTrue());
  }

  @Test
  public void awaitIsInterruptible() {
    var theCount = 1;
    var latch =
        new StoppableCountDownLatch(stopper, theCount, MILLISECONDS.toNanos(2), System::nanoTime);
    var theThread = new AtomicReference<Thread>();

    var latchFuture = executorServiceRule.submit(() -> {
      theThread.set(Thread.currentThread());
      var thrown = catchThrowable(latch::await);
      errorCollector
          .checkSucceeds(() -> assertThat(thrown).isInstanceOf(InterruptedException.class));
    });

    await().until(() -> theThread.get() != null);

    theThread.get().interrupt();

    await().untilAsserted(() -> assertThat(latchFuture.isDone()).isTrue());
    assertThat(latch.getCount()).isEqualTo(theCount);
  }

  @Test
  public void awaitIsCancelable() {
    var theCount = 1;
    var latch =
        new StoppableCountDownLatch(stopper, theCount, MILLISECONDS.toNanos(2), System::nanoTime);
    var theThread = new AtomicReference<Thread>();
    var cancelMessage = "cancel";

    doNothing()
        .doThrow(new CancelException(cancelMessage) {})
        .when(stopper).checkCancelInProgress(any());

    var latchFuture = executorServiceRule.submit(() -> {
      theThread.set(Thread.currentThread());
      var thrown = catchThrowable(latch::await);
      errorCollector.checkSucceeds(
          () -> assertThat(thrown).isInstanceOf(CancelException.class).hasMessage(cancelMessage));
    });

    await().until(() -> theThread.get() != null);

    await().untilAsserted(() -> assertThat(latchFuture.isDone()).isTrue());
    assertThat(latch.getCount()).isEqualTo(theCount);
  }

  @Test
  public void awaitWithTimeoutAndTimeUnitReturnsTrueAfterCountDown() throws Exception {
    var latch =
        new StoppableCountDownLatch(stopper, 1, MILLISECONDS.toNanos(2), System::nanoTime);

    var latchFuture =
        executorServiceRule.submit(() -> latch.await(TIMEOUT_MILLIS, MILLISECONDS));

    latch.countDown();

    assertThat(latchFuture.get(TIMEOUT_MILLIS, MILLISECONDS)).isTrue();
  }

  @Test
  public void awaitWithTimeoutAndTimeUnitReturnsFalseAfterTimeout() throws Exception {
    var latch =
        new StoppableCountDownLatch(stopper, 1, MILLISECONDS.toNanos(2), System::nanoTime);
    long theTimeoutMillis = 2;
    var startNanos = System.nanoTime();

    var latchFuture =
        executorServiceRule.submit(() -> latch.await(theTimeoutMillis, MILLISECONDS));

    assertThat(latchFuture.get(TIMEOUT_MILLIS, MILLISECONDS)).isFalse();
    assertThat(System.nanoTime() - startNanos).isGreaterThanOrEqualTo(theTimeoutMillis);
  }

  @Test
  public void awaitWithTimeoutAndTimeUnitIsInterruptible() {
    var theCount = 1;
    var latch =
        new StoppableCountDownLatch(stopper, theCount, MILLISECONDS.toNanos(2), System::nanoTime);
    var theThread = new AtomicReference<Thread>();

    var latchFuture = executorServiceRule.submit(() -> {
      theThread.set(Thread.currentThread());
      var thrown = catchThrowable(() -> latch.await(TIMEOUT_MILLIS, MILLISECONDS));
      errorCollector
          .checkSucceeds(() -> assertThat(thrown).isInstanceOf(InterruptedException.class));
    });

    await().until(() -> theThread.get() != null);

    theThread.get().interrupt();

    await().untilAsserted(() -> assertThat(latchFuture.isDone()).isTrue());
    assertThat(latch.getCount()).isEqualTo(theCount);
  }

  @Test
  public void awaitWithTimeoutAndTimeUnitIsCancelableAtBeginning() {
    var theCount = 1;
    var latch =
        new StoppableCountDownLatch(stopper, theCount, MILLISECONDS.toNanos(2), System::nanoTime);
    var theThread = new AtomicReference<Thread>();
    var cancelMessage = "cancel";

    doThrow(new CancelException(cancelMessage) {}).when(stopper).checkCancelInProgress(any());

    var latchFuture = executorServiceRule.submit(() -> {
      theThread.set(Thread.currentThread());
      var thrown = catchThrowable(() -> latch.await(TIMEOUT_MILLIS, MILLISECONDS));
      errorCollector.checkSucceeds(
          () -> assertThat(thrown).isInstanceOf(CancelException.class).hasMessage(cancelMessage));
    });

    await().until(() -> theThread.get() != null);

    await().untilAsserted(() -> assertThat(latchFuture.isDone()).isTrue());
    assertThat(latch.getCount()).isEqualTo(theCount);
  }

  @Test
  public void awaitWithTimeoutMillisReturnsTrueAfterCountDown() throws Exception {
    var latch =
        new StoppableCountDownLatch(stopper, 1, MILLISECONDS.toNanos(2), System::nanoTime);

    var latchFuture = executorServiceRule.submit(() -> latch.await(TIMEOUT_MILLIS));

    latch.countDown();

    assertThat(latchFuture.get(TIMEOUT_MILLIS, MILLISECONDS)).isTrue();
  }

  @Test
  public void awaitWithTimeoutMillisReturnsFalseAfterTimeout() throws Exception {
    var latch =
        new StoppableCountDownLatch(stopper, 1, MILLISECONDS.toNanos(2), System::nanoTime);
    long theTimeoutMillis = 2;
    var startNanos = System.nanoTime();

    var latchFuture = executorServiceRule.submit(() -> latch.await(theTimeoutMillis));

    assertThat(latchFuture.get(TIMEOUT_MILLIS, MILLISECONDS)).isFalse();
    assertThat(System.nanoTime() - startNanos).isGreaterThanOrEqualTo(theTimeoutMillis);
  }

  @Test
  public void awaitWithTimeoutMillisIsInterruptible() {
    var theCount = 1;
    var latch =
        new StoppableCountDownLatch(stopper, theCount, MILLISECONDS.toNanos(2), System::nanoTime);
    var theThread = new AtomicReference<Thread>();

    var latchFuture = executorServiceRule.submit(() -> {
      theThread.set(Thread.currentThread());
      var thrown = catchThrowable(() -> latch.await(getTimeout().toMillis()));
      errorCollector
          .checkSucceeds(() -> assertThat(thrown).isInstanceOf(InterruptedException.class));
    });

    await().until(() -> theThread.get() != null);

    theThread.get().interrupt();

    await().untilAsserted(() -> assertThat(latchFuture.isDone()).isTrue());
    assertThat(latch.getCount()).isEqualTo(theCount);
  }

  @Test
  public void awaitWithTimeoutMillisIsCancelableAtBeginning() {
    var theCount = 1;
    var latch =
        new StoppableCountDownLatch(stopper, theCount, MILLISECONDS.toNanos(2), System::nanoTime);
    var theThread = new AtomicReference<Thread>();
    var cancelMessage = "cancel";

    doThrow(new CancelException(cancelMessage) {}).when(stopper).checkCancelInProgress(any());

    var latchFuture = executorServiceRule.submit(() -> {
      theThread.set(Thread.currentThread());
      var thrown = catchThrowable(() -> latch.await(TIMEOUT_MILLIS));
      errorCollector.checkSucceeds(
          () -> assertThat(thrown).isInstanceOf(CancelException.class).hasMessage(cancelMessage));
    });

    await().until(() -> theThread.get() != null);

    await().untilAsserted(() -> assertThat(latchFuture.isDone()).isTrue());
    assertThat(latch.getCount()).isEqualTo(theCount);
  }
}
