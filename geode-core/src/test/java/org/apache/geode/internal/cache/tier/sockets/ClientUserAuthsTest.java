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

package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import org.apache.shiro.subject.Subject;
import org.junit.Test;

public class ClientUserAuthsTest {

  @Test
  public void putSubjectReturnsId() {
    final Subject subject = mock(Subject.class);
    final ClientUserAuths clientUserAuths = new ClientUserAuths();
    final Long id = clientUserAuths.putSubject(subject);
    assertThat(id).isNotNull();
  }

  @Test
  public void getSubjectReturnsSubjectForKnownId() {
    final Subject subject = mock(Subject.class);
    final ClientUserAuths clientUserAuths = new ClientUserAuths();
    final Long id = clientUserAuths.putSubject(subject);

    final Subject knownSubject = clientUserAuths.getSubject(id);

    assertThat(knownSubject).isSameAs(subject);
  }

  @Test
  public void getSubjectReturnsNullForUnknownId() {
    final Subject subject = mock(Subject.class);
    final ClientUserAuths clientUserAuths = new ClientUserAuths();
    final Long id = clientUserAuths.putSubject(subject);

    final Subject unknownSubject = clientUserAuths.getSubject(id + 1);

    assertThat(unknownSubject).isNull();
  }

  @Test
  public void removeSubjectReturnsTrueForKnownId() {
    final Subject subject = mock(Subject.class);
    final ClientUserAuths clientUserAuths = new ClientUserAuths();
    final Long id = clientUserAuths.putSubject(subject);

    final boolean removed = clientUserAuths.removeSubject(id);

    assertThat(removed).isTrue();
    assertThat(clientUserAuths.getSubjects()).isEmpty();

    verify(subject).logout();
  }

  @Test
  public void removeSubjectReturnsFalseForUnknownId() {
    final Subject subject = mock(Subject.class);
    final ClientUserAuths clientUserAuths = new ClientUserAuths();
    final Long id = clientUserAuths.putSubject(subject);

    final boolean removed = clientUserAuths.removeSubject(id + 1);

    assertThat(removed).isFalse();
    assertThat(clientUserAuths.getSubjects()).containsExactly(subject);

    verify(subject, times(0)).logout();
  }

  @Test
  public void cleanupRemovesAllSubjects() {
    final Subject subject = mock(Subject.class);
    final ClientUserAuths clientUserAuths = new ClientUserAuths();
    final Long id = clientUserAuths.putSubject(subject);

    clientUserAuths.cleanup(false);

    assertThat(clientUserAuths.getSubjects()).isEmpty();

    verify(subject).logout();
  }

  @Test
  public void putUniquelyGetsNewKeyIfAlreadyExists() {
    final Supplier<Long> keySource = uncheckedCast(mock(Supplier.class));
    when(keySource.get()).thenReturn(1L, 1L, 2L);
    final ConcurrentMap<Long, Object> map = new ConcurrentHashMap<>();
    final Object value1 = new Object();
    final Object value2 = new Object();

    ClientUserAuths.putUniquely(map, keySource, value1);
    assertThat(map).containsOnly(entry(1L, value1));
    verify(keySource, times(1)).get();

    // noinspection unchecked
    clearInvocations(keySource);

    ClientUserAuths.putUniquely(map, keySource, value2);
    assertThat(map).containsOnly(entry(1L, value1), entry(2L, value2));
    verify(keySource, times(2)).get();
  }
}
