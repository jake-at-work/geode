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

package org.apache.geode.cache.client.internal.pooling;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class ConnectionAccountingTest {
  @Test
  public void constructorSetsMinMax() {
    var a = new ConnectionAccounting(1, 2);
    assertThat(a.getMinimum()).isEqualTo(1);
    assertThat(a.getMaximum()).isEqualTo(2);
    assertThat(a.getCount()).isEqualTo(0);
  }

  @Test
  public void canPrefillWhenUnderMin() {
    var a = new ConnectionAccounting(1, 2);
    assertThat(a.tryPrefill()).isTrue();
    assertThat(a.getCount()).isEqualTo(1);
  }

  @Test
  public void cantPrefillWhenAtMin() {
    var a = new ConnectionAccounting(1, 2);
    a.create();
    assertThat(a.getCount()).isEqualTo(1);

    assertThat(a.tryPrefill()).isFalse();
    assertThat(a.getCount()).isEqualTo(1);
  }

  @Test
  public void cantPrefillWhenAboveMin() {
    var a = new ConnectionAccounting(1, 2);
    a.create();
    a.create();
    assertThat(a.getCount()).isEqualTo(2);

    assertThat(a.tryPrefill()).isFalse();
    assertThat(a.getCount()).isEqualTo(2);
  }

  @Test
  public void cancelPrefillDecrements() {
    var a = new ConnectionAccounting(2, 3);
    a.create();
    assertThat(a.getCount()).isEqualTo(1);

    assertThat(a.tryPrefill()).isTrue();
    assertThat(a.getCount()).isEqualTo(2);

    a.cancelTryPrefill();
    assertThat(a.getCount()).isEqualTo(1);
  }

  @Test
  public void canCreateWhenUnderMax() {
    var a = new ConnectionAccounting(0, 1);
    assertThat(a.getCount()).isEqualTo(0);

    assertThat(a.tryCreate()).isTrue();
    assertThat(a.getCount()).isEqualTo(1);
  }

  @Test
  public void cantCreateWhenAtMax() {
    var a = new ConnectionAccounting(0, 1);
    a.create();
    assertThat(a.getCount()).isEqualTo(1);

    assertThat(a.tryCreate()).isFalse();
    assertThat(a.getCount()).isEqualTo(1);
  }

  @Test
  public void createRegardlessOfMax() {
    var a = new ConnectionAccounting(0, 1);
    a.create();
    assertThat(a.getCount()).isEqualTo(1);

    a.create();
    assertThat(a.getCount()).isEqualTo(2);
    assertThat(a.getMaximum()).isEqualTo(1);
  }

  @Test
  public void cancelCreateDecrementsCount() {
    var a = new ConnectionAccounting(0, 1);
    a.tryCreate();
    assertThat(a.getCount()).isEqualTo(1);

    a.cancelTryCreate();
    assertThat(a.getCount()).isEqualTo(0);
  }

  @Test
  public void tryDestroyDestroysAConnectionOverMax() {
    var a = new ConnectionAccounting(0, 1);
    a.create();
    a.create();
    assertThat(a.getCount()).isEqualTo(2);

    assertThat(a.tryDestroy()).isTrue();
  }

  @Test
  public void tryDoesNotDestroyAtMax() {
    var a = new ConnectionAccounting(0, 1);
    a.create();
    assertThat(a.getCount()).isEqualTo(1);

    assertThat(a.tryDestroy()).isFalse();
  }

  @Test
  public void cancelTryDestroyIncrementsCount() {
    var a = new ConnectionAccounting(0, 1);
    a.create();
    a.create();
    a.tryDestroy();
    assertThat(a.getCount()).isEqualTo(1);

    a.cancelTryDestroy();
    assertThat(a.getCount()).isEqualTo(2);
  }

  @Test
  public void destroyAndIsUnderMinimumReturnsTrueGoingBelowMin() {
    var a = new ConnectionAccounting(1, 2);
    a.create();
    assertThat(a.getCount()).isEqualTo(1);

    assertThat(a.destroyAndIsUnderMinimum(1)).isTrue();
    assertThat(a.getCount()).isEqualTo(0);
  }

  @Test
  public void destroyAndIsUnderMinimumReturnsFalseGoingToMin() {
    var a = new ConnectionAccounting(1, 2);
    a.create();
    a.create();
    assertThat(a.getCount()).isEqualTo(2);

    assertThat(a.destroyAndIsUnderMinimum(1)).isFalse();
    assertThat(a.getCount()).isEqualTo(1);
  }

  @Test
  public void destroyAndIsUnderMinimumReturnsFalseStayingAboveMin() {
    var a = new ConnectionAccounting(1, 2);
    a.create();
    a.create();
    a.create();
    assertThat(a.getCount()).isEqualTo(3);

    assertThat(a.destroyAndIsUnderMinimum(1)).isFalse();
    assertThat(a.getCount()).isEqualTo(2);
  }

  @Test
  public void destroyAndIsUnderMinimumDecrementsByMultiple() {
    var a = new ConnectionAccounting(1, 2);
    a.create();
    a.create();
    a.create();
    assertThat(a.getCount()).isEqualTo(3);

    a.destroyAndIsUnderMinimum(3);
    assertThat(a.getCount()).isEqualTo(0);
  }

  @Test
  public void isUnderMinTrueWhenUnderMin() {
    var a = new ConnectionAccounting(1, 2);
    assertThat(a.isUnderMinimum()).isTrue();
  }

  @Test
  public void isUnderMinFalseWhenAtOrOverMin() {
    var a = new ConnectionAccounting(0, 2);
    assertThat(a.isUnderMinimum()).isFalse();

    a.create();
    assertThat(a.getCount()).isEqualTo(1);
    assertThat(a.isUnderMinimum()).isFalse();
  }

  @Test
  public void isOverMinFalseWhenUnderOrAtMin() {
    var a = new ConnectionAccounting(1, 2);
    assertThat(a.isOverMinimum()).isFalse();

    a.create();
    assertThat(a.getCount()).isEqualTo(1);
    assertThat(a.isOverMinimum()).isFalse();
  }

  @Test
  public void isOverMinTrueWhenOverMin() {
    var a = new ConnectionAccounting(1, 2);
    a.create();
    a.create();
    assertThat(a.getCount()).isEqualTo(2);

    assertThat(a.isOverMinimum()).isTrue();
  }
}
