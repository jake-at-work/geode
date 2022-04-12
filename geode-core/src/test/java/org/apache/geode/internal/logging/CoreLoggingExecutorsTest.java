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
package org.apache.geode.internal.logging;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.internal.FunctionExecutionPooledExecutor;
import org.apache.geode.distributed.internal.FunctionExecutionPooledExecutor.FunctionExecutionRejectedExecutionHandler;
import org.apache.geode.distributed.internal.OverflowQueueWithDMStats;
import org.apache.geode.distributed.internal.PoolStatHelper;
import org.apache.geode.distributed.internal.PooledExecutorWithDMStats;
import org.apache.geode.distributed.internal.QueueStatHelper;
import org.apache.geode.distributed.internal.SerialQueuedExecutorWithDMStats;
import org.apache.geode.internal.ScheduledThreadPoolExecutorWithKeepAlive;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.executors.LoggingThreadFactory;
import org.apache.geode.logging.internal.executors.LoggingThreadFactory.CommandWrapper;
import org.apache.geode.logging.internal.executors.LoggingThreadFactory.ThreadInitializer;

public class CoreLoggingExecutorsTest {

  private CommandWrapper commandWrapper;
  private PoolStatHelper poolStatHelper;
  private QueueStatHelper queueStatHelper;
  private Runnable runnable;
  private ThreadInitializer threadInitializer;
  private ThreadsMonitoring threadsMonitoring;

  @Before
  public void setUp() {
    commandWrapper = mock(CommandWrapper.class);
    poolStatHelper = mock(PoolStatHelper.class);
    queueStatHelper = mock(QueueStatHelper.class);
    runnable = mock(Runnable.class);
    threadInitializer = mock(ThreadInitializer.class);
    threadsMonitoring = mock(ThreadsMonitoring.class);
  }

  /**
   * Creates, passes in, and uses OverflowQueueWithDMStats.
   *
   * <p>
   * Uses {@code ThreadPoolExecutor.AbortPolicy}.
   */
  @Test
  public void newFixedThreadPoolWithTimeout() {
    var poolSize = 5;
    var keepAliveTime = 2;
    var threadName = "thread";

    var executorService = CoreLoggingExecutors
        .newFixedThreadPoolWithTimeout(poolSize, keepAliveTime, MINUTES, queueStatHelper,
            threadName);

    assertThat(executorService).isInstanceOf(ThreadPoolExecutor.class);

    var executor = (ThreadPoolExecutor) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(poolSize);
    assertThat(executor.getKeepAliveTime(MINUTES)).isEqualTo(keepAliveTime);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(poolSize);
    assertThat(executor.getQueue()).isInstanceOf(OverflowQueueWithDMStats.class);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(ThreadPoolExecutor.AbortPolicy.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    var overflowQueueWithDMStats =
        (OverflowQueueWithDMStats) executor.getQueue();

    assertThat(overflowQueueWithDMStats.getQueueStatHelper()).isSameAs(queueStatHelper);

    var threadFactory = executor.getThreadFactory();
    var thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  /**
   * Creates and passes in OverflowQueueWithDMStats. FunctionExecutionPooledExecutor then creates
   * and passes up SynchronousQueue, but finally uses OverflowQueueWithDMStats.
   *
   * <p>
   * {@code getBufferQueue()} returns passed-in OverflowQueueWithDMStats. {@code getQueue()} returns
   * internal SynchronousQueue.
   *
   * <p>
   * Uses {@code ThreadPoolExecutor.AbortPolicy}.
   */
  @Test
  public void newFunctionThreadPoolWithFeedStatistics() {
    var poolSize = 5;
    var workQueueSize = 2;
    var threadName = "thread";

    var executorService = CoreLoggingExecutors
        .newFunctionThreadPoolWithFeedStatistics(poolSize, workQueueSize, queueStatHelper,
            threadName, threadInitializer, commandWrapper, poolStatHelper, threadsMonitoring);

    assertThat(executorService).isInstanceOf(FunctionExecutionPooledExecutor.class);

    var executor = (FunctionExecutionPooledExecutor) executorService;

    assertThat(executor.getBufferQueue()).isInstanceOf(OverflowQueueWithDMStats.class);
    assertThat(executor.getCorePoolSize()).isEqualTo(1);
    assertThat(executor.getKeepAliveTime(MINUTES)).isEqualTo(30);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(poolSize);
    assertThat(executor.getQueue()).isInstanceOf(SynchronousQueue.class);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(FunctionExecutionRejectedExecutionHandler.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    var overflowQueueWithDMStats =
        (OverflowQueueWithDMStats) executor.getBufferQueue();

    assertThat(overflowQueueWithDMStats.getQueueStatHelper()).isSameAs(queueStatHelper);

    var threadFactory = executor.getThreadFactory();
    var thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  /**
   * Passes in and uses BlockingQueue.
   *
   * <p>
   * Uses {@code PooledExecutorWithDMStats.BlockHandler}.
   */
  @Test
  public void newSerialThreadPool() {
    BlockingQueue<Runnable> workQueue = mock(BlockingQueue.class);
    var threadName = "thread";

    var executorService = CoreLoggingExecutors
        .newSerialThreadPool(workQueue, threadName, threadInitializer, commandWrapper,
            poolStatHelper, threadsMonitoring);

    assertThat(executorService).isInstanceOf(SerialQueuedExecutorWithDMStats.class);

    var executor = (SerialQueuedExecutorWithDMStats) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(1);
    assertThat(executor.getKeepAliveTime(MINUTES)).isEqualTo(1);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(1);
    assertThat(executor.getPoolStatHelper()).isSameAs(poolStatHelper);
    assertThat(executor.getQueue()).isSameAs(workQueue);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(PooledExecutorWithDMStats.BlockHandler.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    var threadFactory = executor.getThreadFactory();
    var thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  /**
   * Creates, passes in, and uses OverflowQueueWithDMStats.
   *
   * <p>
   * Uses {@code PooledExecutorWithDMStats.BlockHandler}.
   */
  @Test
  public void newSerialThreadPoolWithFeedStatistics() {
    var workQueueSize = 2;
    var threadName = "thread";

    var executorService = CoreLoggingExecutors
        .newSerialThreadPoolWithFeedStatistics(workQueueSize, queueStatHelper, threadName,
            threadInitializer, commandWrapper, poolStatHelper, threadsMonitoring);

    assertThat(executorService).isInstanceOf(SerialQueuedExecutorWithDMStats.class);

    var executor = (SerialQueuedExecutorWithDMStats) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(1);
    assertThat(executor.getKeepAliveTime(MINUTES)).isEqualTo(1);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(1);
    assertThat(executor.getPoolStatHelper()).isSameAs(poolStatHelper);
    assertThat(executor.getQueue()).isInstanceOf(OverflowQueueWithDMStats.class);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(PooledExecutorWithDMStats.BlockHandler.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    var threadFactory = executor.getThreadFactory();
    var thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  /**
   * Creates, passes up, and uses SynchronousQueue. ScheduledThreadPoolExecutorWithKeepAlive then
   * creates a ThreadPoolExecutor to delegate to.
   *
   * <p>
   * Uses {@code ScheduledThreadPoolExecutorWithKeepAlive.BlockCallerPolicy}.
   */
  @Test
  public void newScheduledThreadPool() {
    var poolSize = 10;
    long keepAliveTime = 5000;
    var unit = SECONDS;
    var threadName = "thread";

    ExecutorService executorService = CoreLoggingExecutors
        .newScheduledThreadPool(poolSize, keepAliveTime, unit, threadName, threadsMonitoring);

    assertThat(executorService).isInstanceOf(ScheduledThreadPoolExecutorWithKeepAlive.class);

    var executor =
        (ScheduledThreadPoolExecutorWithKeepAlive) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(1);
    assertThat(executor.getDelegateExecutor()).isInstanceOf(ScheduledThreadPoolExecutor.class);
    assertThat(executor.getKeepAliveTime(unit)).isEqualTo(keepAliveTime);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(poolSize);
    assertThat(executor.getQueue()).isInstanceOf(SynchronousQueue.class);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(ScheduledThreadPoolExecutorWithKeepAlive.BlockCallerPolicy.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    var delegate = executor.getDelegateExecutor();

    assertThat(delegate.getContinueExistingPeriodicTasksAfterShutdownPolicy()).isFalse();
    assertThat(delegate.getExecuteExistingDelayedTasksAfterShutdownPolicy()).isFalse();

    var threadFactory = executor.getThreadFactory();
    var thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  /**
   * Creates and passes up BlockingQueue. PooledExecutorWithDMStats then creates and passes up
   * SynchronousQueue
   */
  @Test
  public void newThreadPool() {
    var poolSize = 10;
    BlockingQueue<Runnable> workQueue = mock(BlockingQueue.class);
    var threadName = "thread";

    var executorService = CoreLoggingExecutors
        .newThreadPool(poolSize, workQueue, threadName, threadInitializer, commandWrapper,
            poolStatHelper, threadsMonitoring);

    assertThat(executorService).isInstanceOf(PooledExecutorWithDMStats.class);

    var executor = (PooledExecutorWithDMStats) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(1);
    assertThat(executor.getKeepAliveTime(MINUTES)).isEqualTo(30);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(poolSize);
    assertThat(executor.getQueue()).isInstanceOf(SynchronousQueue.class);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(PooledExecutorWithDMStats.BufferHandler.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    var threadFactory = executor.getThreadFactory();
    var thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  /**
   * Creates ArrayBlockingQueue but then uses SynchronousQueue
   */
  @Test
  public void newThreadPoolWithFixedFeed() {
    var poolSize = 10;
    long keepAliveTime = 5000;
    var unit = SECONDS;
    var workQueueSize = 2;
    var threadName = "thread";

    var executorService = CoreLoggingExecutors
        .newThreadPoolWithFixedFeed(poolSize, keepAliveTime, unit, workQueueSize, threadName,
            commandWrapper, poolStatHelper, threadsMonitoring);

    assertThat(executorService).isInstanceOf(PooledExecutorWithDMStats.class);

    var executor =
        (PooledExecutorWithDMStats) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(1);
    assertThat(executor.getKeepAliveTime(unit)).isEqualTo(keepAliveTime);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(poolSize);
    assertThat(executor.getQueue()).isInstanceOf(SynchronousQueue.class);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(PooledExecutorWithDMStats.BufferHandler.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    var threadFactory = executor.getThreadFactory();
    var thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  /**
   * Creates and passes in OverflowQueueWithDMStats. PooledExecutorWithDMStats then creates and
   * uses SynchronousQueue.
   */
  @Test
  public void newThreadPoolWithFeedStatistics() {
    var poolSize = 10;
    var workQueueSize = 2;
    var threadName = "thread";

    var executorService = CoreLoggingExecutors
        .newThreadPoolWithFeedStatistics(poolSize, workQueueSize, queueStatHelper, threadName,
            threadInitializer, commandWrapper, poolStatHelper, threadsMonitoring);

    assertThat(executorService).isInstanceOf(PooledExecutorWithDMStats.class);

    var executor =
        (PooledExecutorWithDMStats) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(1);
    assertThat(executor.getKeepAliveTime(MINUTES)).isEqualTo(30);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(poolSize);
    assertThat(executor.getQueue()).isInstanceOf(SynchronousQueue.class);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(PooledExecutorWithDMStats.BufferHandler.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    var threadFactory = executor.getThreadFactory();
    var thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  @Test
  public void newThreadPoolWithSynchronousFeed() {
    var poolSize = 10;
    var threadName = "thread";

    var executorService = CoreLoggingExecutors.newThreadPoolWithSynchronousFeed(
        poolSize, threadName, commandWrapper);

    assertThat(executorService).isInstanceOf(PooledExecutorWithDMStats.class);

    var executor =
        (PooledExecutorWithDMStats) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(1);
    assertThat(executor.getKeepAliveTime(MINUTES)).isEqualTo(30);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(poolSize);
    assertThat(executor.getQueue()).isInstanceOf(SynchronousQueue.class);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(ThreadPoolExecutor.CallerRunsPolicy.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    var threadFactory = executor.getThreadFactory();
    var thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  @Test
  public void newThreadPoolWithSynchronousFeed_2() {
    var poolSize = 10;
    long keepAliveTime = 5000;
    var unit = SECONDS;
    var threadName = "thread";

    var executorService = CoreLoggingExecutors.newThreadPoolWithSynchronousFeed(
        poolSize, keepAliveTime, unit, threadName, commandWrapper, poolStatHelper,
        threadsMonitoring);

    assertThat(executorService).isInstanceOf(PooledExecutorWithDMStats.class);

    var executor =
        (PooledExecutorWithDMStats) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(1);
    assertThat(executor.getKeepAliveTime(unit)).isEqualTo(keepAliveTime);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(poolSize);
    assertThat(executor.getQueue()).isInstanceOf(SynchronousQueue.class);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(ThreadPoolExecutor.CallerRunsPolicy.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    var threadFactory = executor.getThreadFactory();
    var thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  @Test
  public void newThreadPoolWithSynchronousFeed_3() {
    var poolSize = 10;
    long keepAliveTime = 5000;
    var unit = SECONDS;
    var threadName = "thread";
    var rejectedExecutionHandler = mock(RejectedExecutionHandler.class);

    var executorService = CoreLoggingExecutors.newThreadPoolWithSynchronousFeed(
        poolSize, keepAliveTime, unit, threadName, rejectedExecutionHandler, poolStatHelper);

    assertThat(executorService).isInstanceOf(PooledExecutorWithDMStats.class);

    var executor =
        (PooledExecutorWithDMStats) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(1);
    assertThat(executor.getKeepAliveTime(unit)).isEqualTo(keepAliveTime);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(poolSize);
    assertThat(executor.getQueue()).isInstanceOf(SynchronousQueue.class);
    assertThat(executor.getRejectedExecutionHandler()).isSameAs(rejectedExecutionHandler);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    var threadFactory = executor.getThreadFactory();
    var thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  @Test
  public void newThreadPoolWithSynchronousFeed_4() {
    var corePoolSize = 10;
    var maximumPoolSize = 20;
    long keepAliveTime = 5000;
    var unit = SECONDS;
    var threadName = "thread";

    var executorService = CoreLoggingExecutors.newThreadPoolWithSynchronousFeed(
        corePoolSize, maximumPoolSize, keepAliveTime, unit, threadName, threadInitializer,
        commandWrapper);

    assertThat(executorService).isInstanceOf(ThreadPoolExecutor.class);

    var executor = (ThreadPoolExecutor) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(corePoolSize);
    assertThat(executor.getKeepAliveTime(unit)).isEqualTo(keepAliveTime);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(maximumPoolSize);
    assertThat(executor.getQueue()).isInstanceOf(SynchronousQueue.class);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(ThreadPoolExecutor.AbortPolicy.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    var threadFactory = executor.getThreadFactory();
    var thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  /**
   * Used for P2P Reader Threads in ConnectionTable
   */
  @Test
  public void newThreadPoolWithSynchronousFeed_5() {
    var corePoolSize = 10;
    var maximumPoolSize = 20;
    long keepAliveTime = 5000;
    var unit = SECONDS;
    var threadName = "thread";

    var executorService = CoreLoggingExecutors.newThreadPoolWithSynchronousFeed(
        corePoolSize, maximumPoolSize, keepAliveTime, unit, threadName);

    assertThat(executorService).isInstanceOf(ThreadPoolExecutor.class);

    var executor = (ThreadPoolExecutor) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(corePoolSize);
    assertThat(executor.getKeepAliveTime(unit)).isEqualTo(keepAliveTime);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(maximumPoolSize);
    assertThat(executor.getQueue()).isInstanceOf(SynchronousQueue.class);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(ThreadPoolExecutor.AbortPolicy.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    var threadFactory = executor.getThreadFactory();
    var thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  @Test
  public void newThreadPoolWithSynchronousFeedThatHandlesRejection() {
    var corePoolSize = 10;
    var maximumPoolSize = 20;
    long keepAliveTime = 5000;
    var unit = SECONDS;
    var threadName = "thread";

    var executorService = CoreLoggingExecutors
        .newThreadPoolWithSynchronousFeedThatHandlesRejection(corePoolSize, maximumPoolSize,
            keepAliveTime, unit, threadName, threadInitializer, commandWrapper);

    assertThat(executorService).isInstanceOf(ThreadPoolExecutor.class);

    var executor = (ThreadPoolExecutor) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(corePoolSize);
    assertThat(executor.getKeepAliveTime(unit)).isEqualTo(keepAliveTime);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(maximumPoolSize);
    assertThat(executor.getQueue()).isInstanceOf(SynchronousQueue.class);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(CoreLoggingExecutors.QueuingRejectedExecutionHandler.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    var threadFactory = executor.getThreadFactory();
    var thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }

  @Test
  public void newThreadPoolWithUnlimitedFeed() {
    var poolSize = 10;
    long keepAliveTime = 5000;
    var unit = SECONDS;
    var threadName = "thread";

    var executorService = CoreLoggingExecutors.newThreadPoolWithUnlimitedFeed(
        poolSize, keepAliveTime, unit, threadName, threadInitializer, commandWrapper,
        poolStatHelper, threadsMonitoring);

    assertThat(executorService).isInstanceOf(ThreadPoolExecutor.class);

    var executor = (ThreadPoolExecutor) executorService;

    assertThat(executor.getCorePoolSize()).isEqualTo(1);
    assertThat(executor.getKeepAliveTime(unit)).isEqualTo(keepAliveTime);
    assertThat(executor.getMaximumPoolSize()).isEqualTo(poolSize);
    assertThat(executor.getQueue()).isInstanceOf(SynchronousQueue.class);
    assertThat(executor.getRejectedExecutionHandler())
        .isInstanceOf(PooledExecutorWithDMStats.BufferHandler.class);
    assertThat(executor.getThreadFactory()).isInstanceOf(LoggingThreadFactory.class);

    var threadFactory = executor.getThreadFactory();
    var thread = threadFactory.newThread(runnable);

    assertThat(thread).isInstanceOf(LoggingThread.class);
    assertThat(thread.getName()).contains(threadName);
  }
}
