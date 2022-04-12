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
 *
 */

package org.apache.geode.management.internal.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Date;

import org.junit.Test;

import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.runtime.OperationResult;


public class OperationStateTest {

  @Test
  public void getId() {
    OperationState<?, ?> operationState = new OperationState<>("opId", null, null);
    assertThat(operationState.getId()).isEqualTo("opId");
  }

  @Test
  public void getOperation() {
    ClusterManagementOperation<?> operation = mock(ClusterManagementOperation.class);
    OperationState<?, ?> operationState = new OperationState<>(null, operation, null);
    assertThat(operationState.getOperation()).isSameAs(operation);
  }

  @Test
  public void getOperationStart() {
    var start = new Date();
    OperationState<?, ?> operationState = new OperationState<>(null, null, start);
    assertThat(operationState.getOperationStart()).isSameAs(start);
  }

  @Test
  public void getOperationEnd() {
    var end = new Date();
    OperationState<?, ?> operationState = new OperationState<>(null, null, null);
    assertThat(operationState.getOperationEnd()).isNull();
    operationState.setOperationEnd(end, null, null);
    assertThat(operationState.getOperationEnd()).isSameAs(end);
  }

  @Test
  public void getLocator() {
    var locator = "locator";
    var operationState = new OperationState(null, null, null);
    operationState.setLocator(locator);
    assertThat(operationState.getLocator()).isSameAs(locator);
  }

  @Test
  public void getResult() {
    OperationState<?, OperationResult> operationState = new OperationState<>(null, null, null);
    assertThat(operationState.getResult()).isNull();
    var result = mock(OperationResult.class);
    operationState.setOperationEnd(null, result, null);
    assertThat(operationState.getResult()).isSameAs(result);
  }

  @Test
  public void getThrowable() {
    OperationState<?, ?> operationState = new OperationState<>(null, null, null);
    assertThat(operationState.getThrowable()).isNull();
    var throwable = new Throwable();
    operationState.setOperationEnd(null, null, throwable);
    assertThat(operationState.getThrowable()).isSameAs(throwable);
  }

  @Test
  public void verifyEqualInstances() {
    OperationState<?, OperationResult> operationState1 = new OperationState<>(null, null, null);
    OperationState<?, OperationResult> operationState2 = new OperationState<>(null, null, null);
    assertThat(operationState1).isEqualTo(operationState2);
    var operation = mock(ClusterManagementOperation.class);
    var start = new Date();
    var end = new Date();
    var result = mock(OperationResult.class);
    var throwable = new Throwable();
    operationState1 = new OperationState<>("opId", operation, start);
    operationState1.setOperationEnd(end, result, throwable);
    operationState2 = new OperationState<>("opId", operation, start);
    operationState2.setOperationEnd(end, result, throwable);
    assertThat(operationState1).isEqualTo(operationState2);
  }

  @Test
  public void verifyHashCode() {
    OperationState<?, OperationResult> operationState = new OperationState<>(null, null, null);
    operationState.hashCode();
    operationState = new OperationState<>("opId", null, null);
    var hashCode = operationState.hashCode();
    var operation = mock(ClusterManagementOperation.class);
    var end = new Date();
    var result = mock(OperationResult.class);
    var throwable = new Throwable();
    operationState.setOperationEnd(end, result, throwable);
    assertThat(operationState.hashCode()).isEqualTo(hashCode);
  }

  @Test
  public void verifyNonEqualInstances() {
    OperationState<?, OperationResult> operationState1 = new OperationState<>("opId1", null, null);
    OperationState<?, OperationResult> operationState2 = new OperationState<>("opId2", null, null);
    assertThat(operationState1).isNotEqualTo(operationState2);
  }

  @Test
  public void createCopyProducesEqualInstance() {
    var operation = mock(ClusterManagementOperation.class);
    var start = new Date();
    var end = new Date();
    OperationState<?, OperationResult> operationState =
        new OperationState<>("opId", operation, start);
    var result = mock(OperationResult.class);
    var throwable = new Throwable();
    operationState.setOperationEnd(end, result, throwable);

    var operationStateCopy = operationState.createCopy();

    assertThat(operationStateCopy).isEqualTo(operationState);
  }

  @Test
  public void createCopyResultIsNotChangedBySubsequentOperationEndCalls() {
    var operation = mock(ClusterManagementOperation.class);
    var start = new Date();
    var end = new Date();
    OperationState<?, OperationResult> operationState =
        new OperationState<>("opId", operation, start);

    var operationStateCopy = operationState.createCopy();
    operationState.setOperationEnd(end, null, null);

    assertThat(operationStateCopy).isNotSameAs(operationState);
    assertThat(operationState.getOperationEnd()).isNotNull();
    assertThat(operationStateCopy.getOperationEnd()).isNull();
  }

  @Test
  public void testToString() {
    var operation = mock(ClusterManagementOperation.class);
    var start = new Date();
    var end = new Date();
    OperationState<?, OperationResult> operationState =
        new OperationState<>("opId", operation, start);
    operationState.setOperationEnd(end, null, null);

    final var expected = "OperationState{" +
        "opId=" + operationState.getId() +
        ", operation=" + operationState.getOperation() +
        ", operationStart=" + operationState.getOperationStart() +
        ", operationEnd=" + operationState.getOperationEnd() +
        ", result=null" +
        ", throwable=null" +
        ", locator=" + operationState.getLocator() +
        '}';

    assertThat(operationState.toString()).isEqualTo(expected);
  }

  @Test
  public void testToStringWithThrowable() {
    var operation = mock(ClusterManagementOperation.class);
    var start = new Date();
    var end = new Date();
    OperationState<?, OperationResult> operationState =
        new OperationState<>("opId", operation, start);
    operationState.setOperationEnd(end, null, new RuntimeException("Test"));

    final var expected = "OperationState{" +
        "opId=" + operationState.getId() +
        ", operation=" + operationState.getOperation() +
        ", operationStart=" + operationState.getOperationStart() +
        ", operationEnd=" + operationState.getOperationEnd() +
        ", result=null" +
        ", throwable=" + operationState.getThrowable().getMessage() +
        ", locator=" + operationState.getLocator() +
        '}';

    assertThat(operationState.toString()).isEqualTo(expected);
  }

}
