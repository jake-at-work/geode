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
package org.apache.geode.test.junit.rules;

import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.junit.runners.TestRunner;

/**
 * Unit tests for {@link ExpectedTimeoutRule}.
 */
public class ExpectedTimeoutRuleTest {

  private static final long TIMEOUT_MILLIS = getTimeout().toMillis();

  @Test
  public void passesUnused() {
    var result = TestRunner.runTest(PassingTestShouldPassWhenUnused.class);

    assertThat(result.wasSuccessful()).isTrue();
  }

  @Test
  public void failsWithoutExpectedException() {
    var result = TestRunner.runTest(FailsWithoutExpectedException.class);

    assertThat(result.wasSuccessful()).isFalse();

    var failures = result.getFailures();
    assertThat(failures.size()).as("Failures: " + failures).isEqualTo(1);

    var failure = failures.get(0);
    assertThat(failure.getException()).isExactlyInstanceOf(AssertionError.class)
        .hasMessage("Expected test to throw an instance of " + TimeoutException.class.getName());
  }

  @Test
  public void failsWithoutExpectedTimeoutException() {
    var result = TestRunner.runTest(FailsWithoutExpectedTimeoutException.class);

    assertThat(result.wasSuccessful()).isFalse();

    var failures = result.getFailures();
    assertThat(failures.size()).as("Failures: " + failures).isEqualTo(1);

    var failure = failures.get(0);
    assertThat(failure.getException()).isExactlyInstanceOf(AssertionError.class)
        .hasMessage("Expected test to throw (an instance of " + TimeoutException.class.getName()
            + " and exception with message a string containing \""
            + FailsWithoutExpectedTimeoutException.message + "\")");
  }

  @Test
  public void failsWithExpectedTimeoutButWrongError() {
    var result = TestRunner.runTest(FailsWithExpectedTimeoutButWrongError.class);

    assertThat(result.wasSuccessful()).isFalse();

    var failures = result.getFailures();
    assertThat(failures.size()).as("Failures: " + failures).isEqualTo(1);

    var failure = failures.get(0);
    var expectedMessage = System.lineSeparator()
        + "Expected: (an instance of java.util.concurrent.TimeoutException and exception with message a string containing \"this is a message for FailsWithExpectedTimeoutButWrongError\")"
        + System.lineSeparator() + "     "
        + "but: an instance of java.util.concurrent.TimeoutException <java.lang.NullPointerException> is a java.lang.NullPointerException";
    assertThat(failure.getException()).isExactlyInstanceOf(AssertionError.class)
        .hasMessageContaining(expectedMessage);
  }

  @Test
  public void passesWithExpectedTimeoutAndTimeoutException() {
    var result = TestRunner.runTest(PassesWithExpectedTimeoutAndTimeoutException.class);

    assertThat(result.wasSuccessful()).isTrue();
  }

  @Test
  public void failsWhenTimeoutIsEarly() {
    var result = TestRunner.runTest(FailsWhenTimeoutIsEarly.class);

    assertThat(result.wasSuccessful()).isFalse();

    var failures = result.getFailures();
    assertThat(failures.size()).as("Failures: " + failures).isEqualTo(1);

    var failure = failures.get(0);
    assertThat(failure.getException()).isExactlyInstanceOf(AssertionError.class)
        .hasMessage("Expected test to throw (an instance of " + TimeoutException.class.getName()
            + " and exception with message a string containing \"" + FailsWhenTimeoutIsEarly.message
            + "\")");
  }

  @Test
  public void failsWhenTimeoutIsLate() {
    var result = TestRunner.runTest(FailsWhenTimeoutIsLate.class);

    assertThat(result.wasSuccessful()).isFalse();

    var failures = result.getFailures();
    assertThat(failures.size()).as("Failures: " + failures).isEqualTo(1);

    var failure = failures.get(0);
    assertThat(failure.getException()).isExactlyInstanceOf(AssertionError.class)
        .hasMessage("Expected test to throw (an instance of " + TimeoutException.class.getName()
            + " and exception with message a string containing \"" + FailsWhenTimeoutIsLate.message
            + "\")");
  }

  /**
   * Base class for all inner class test cases
   */
  public static class AbstractExpectedTimeoutRuleTest {

    @Rule
    public ExpectedTimeoutRule timeout = ExpectedTimeoutRule.none();
  }

  /**
   * Used by test {@link #passesUnused()}
   */
  public static class PassingTestShouldPassWhenUnused extends AbstractExpectedTimeoutRuleTest {

    @Test
    public void doTest() {}
  }

  /**
   * Used by test {@link #failsWithoutExpectedException()}
   */
  public static class FailsWithoutExpectedException extends AbstractExpectedTimeoutRuleTest {

    @Test
    public void doTest() {
      timeout.expect(TimeoutException.class);
    }
  }

  /**
   * Used by test {@link #failsWithoutExpectedTimeoutException()}
   */
  public static class FailsWithoutExpectedTimeoutException extends AbstractExpectedTimeoutRuleTest {

    static final String message = "this is a message for FailsWithoutExpectedTimeoutException";

    @Test
    public void doTest() throws Exception {
      timeout.expect(TimeoutException.class);
      timeout.expectMessage(message);
      timeout.expectMinimumDuration(10);
      timeout.expectMaximumDuration(TIMEOUT_MILLIS);
      timeout.expectTimeUnit(TimeUnit.MILLISECONDS);
      Thread.sleep(100);
    }
  }

  /**
   * Used by test {@link #failsWithExpectedTimeoutButWrongError()}
   */
  public static class FailsWithExpectedTimeoutButWrongError
      extends AbstractExpectedTimeoutRuleTest {

    static final String message = "this is a message for FailsWithExpectedTimeoutButWrongError";

    @Test
    public void doTest() throws Exception {
      timeout.expect(TimeoutException.class);
      timeout.expectMessage(message);
      timeout.expectMinimumDuration(10);
      timeout.expectMaximumDuration(TIMEOUT_MILLIS);
      timeout.expectTimeUnit(TimeUnit.MILLISECONDS);
      Thread.sleep(100);
      throw new NullPointerException();
    }
  }

  /**
   * Used by test {@link #passesWithExpectedTimeoutAndTimeoutException()}
   */
  public static class PassesWithExpectedTimeoutAndTimeoutException
      extends AbstractExpectedTimeoutRuleTest {

    static final String message =
        "this is a message for PassesWithExpectedTimeoutAndTimeoutException";
    static final Class<TimeoutException> exceptionClass = TimeoutException.class;

    @Test
    public void doTest() throws Exception {
      timeout.expect(exceptionClass);
      timeout.expectMessage(message);
      timeout.expectMinimumDuration(10);
      timeout.expectMaximumDuration(TIMEOUT_MILLIS);
      timeout.expectTimeUnit(TimeUnit.MILLISECONDS);
      Thread.sleep(100);
      throw new TimeoutException(message);
    }
  }

  /**
   * Used by test {@link #failsWhenTimeoutIsEarly()}
   */
  public static class FailsWhenTimeoutIsEarly extends AbstractExpectedTimeoutRuleTest {

    static final String message = "this is a message for FailsWhenTimeoutIsEarly";

    @Test
    public void doTest() throws Exception {
      timeout.expect(TimeoutException.class);
      timeout.expectMessage(message);
      timeout.expectMinimumDuration(TIMEOUT_MILLIS / 2);
      timeout.expectMaximumDuration(TIMEOUT_MILLIS);
      timeout.expectTimeUnit(TimeUnit.MILLISECONDS);
      Thread.sleep(10);
    }
  }

  /**
   * Used by test {@link #failsWhenTimeoutIsLate()}
   */
  public static class FailsWhenTimeoutIsLate extends AbstractExpectedTimeoutRuleTest {

    static final String message = "this is a message for FailsWhenTimeoutIsLate";

    @Test
    public void doTest() throws Exception {
      timeout.expect(TimeoutException.class);
      timeout.expectMessage(message);
      timeout.expectMinimumDuration(10);
      timeout.expectMaximumDuration(20);
      timeout.expectTimeUnit(TimeUnit.MILLISECONDS);
      Thread.sleep(100);
    }
  }
}
