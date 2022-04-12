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

package org.apache.geode.internal.serialization;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class VersioningJUnitTest {

  @Test
  public void getVersionForKnownVersion() {
    final var current = KnownVersion.getCurrentVersion();
    final var knownVersion = Versioning.getVersion(current.ordinal());
    assertThat(knownVersion).isInstanceOf(KnownVersion.class);
    assertThat(knownVersion).isEqualTo(current);
  }

  @Test
  public void getVersionForUnknownVersion() {
    // Version.getCurrentVersion() returns the newest/latest version
    final var unknownOrdinal = (short) (KnownVersion.getCurrentVersion().ordinal() + 1);
    final var unknownVersion = Versioning.getVersion(unknownOrdinal);
    assertThat(unknownVersion).isInstanceOf(UnknownVersion.class);
  }

  @Test
  public void getVersionForToken() {
    final var versionOrdinal = Versioning.getVersion(KnownVersion.TOKEN_ORDINAL);
    assertThat(versionOrdinal).isEqualTo(KnownVersion.TOKEN);
    assertThat(versionOrdinal).isInstanceOf(KnownVersion.class);
  }

  @Test
  public void getVersionForUnknownNegativeShort() {
    // a little coziness with TOKEN_ORDINAL: we happen to know ordinals lower than that are not
    // known versions
    final var versionOrdinal =
        Versioning.getVersion((short) (KnownVersion.TOKEN_ORDINAL - 1));
    assertThat(versionOrdinal).isInstanceOf(UnknownVersion.class);
  }

  @Test
  public void getKnownVersionForKnownVersion() {
    final var current = KnownVersion.getCurrentVersion();
    final var knownVersion = Versioning.getKnownVersionOrDefault(current, null);
    assertThat(knownVersion).isEqualTo(current);
  }

  @Test
  public void getKnownVersionForUnknownVersion() {
    // Version.getCurrentVersion() returns the newest/latest version
    final var current = KnownVersion.getCurrentVersion();
    final var unknownOrdinal = (short) (current.ordinal() + 1);
    final var unknownVersion = new UnknownVersion(unknownOrdinal);
    assertThat(Versioning.getKnownVersionOrDefault(unknownVersion, null)).isNull();
    assertThat(Versioning.getKnownVersionOrDefault(unknownVersion, current)).isEqualTo(current);
  }

}
