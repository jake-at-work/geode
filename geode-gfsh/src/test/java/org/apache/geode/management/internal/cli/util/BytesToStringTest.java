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
package org.apache.geode.management.internal.cli.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;


public class BytesToStringTest {
  private static final long KB = 1024;
  private static final long MB = 1024 * KB;
  private static final long GB = 1024 * MB;
  private static final long TB = 1024 * GB;

  private BytesToString bytesToString;

  @Before
  public void before() {
    bytesToString = new BytesToString();
  }

  @Test
  public void testFiveHundredBytes() {
    var value = bytesToString.of(500);
    assertThat(value).isEqualTo("500 Byte(s)");
  }

  @Test
  public void testOneKb() {
    var value = bytesToString.of(KB);
    assertThat(value).isEqualTo("1 KB");
  }

  @Test
  public void testThreeKb() {
    var value = bytesToString.of(3 * KB);
    assertThat(value).isEqualTo("3 KB");
  }

  @Test
  public void testFractionalKB() {
    var value = bytesToString.of(KB + 500);
    assertThat(value).isEqualTo("1.49 KB");
  }

  @Test
  public void testOneMB() {
    var value = bytesToString.of(MB);
    assertThat(value).isEqualTo("1 MB");
  }

  @Test
  public void testThreeMB() {
    var value = bytesToString.of(3 * MB);
    assertThat(value).isEqualTo("3 MB");
  }

  @Test
  public void testFractionalMB() {
    var value = bytesToString.of(MB + 500 * KB);
    assertThat(value).isEqualTo("1.49 MB");
  }

  @Test
  public void testOneGB() {
    var value = bytesToString.of(GB);
    assertThat(value).isEqualTo("1 GB");
  }

  @Test
  public void testThreeGB() {
    var value = bytesToString.of(3 * GB);
    assertThat(value).isEqualTo("3 GB");
  }

  @Test
  public void testFractionalGB() {
    var value = bytesToString.of(GB + 500 * MB);
    assertThat(value).isEqualTo("1.49 GB");
  }

  @Test
  public void testOneTB() {
    var value = bytesToString.of(TB);
    assertThat(value).isEqualTo("1 TB");
  }

  @Test
  public void testThreeTB() {
    var value = bytesToString.of(3 * GB);
    assertThat(value).isEqualTo("3 GB");
  }

  @Test
  public void testFractionalTB() {
    var value = bytesToString.of(TB + 500 * GB);
    assertThat(value).isEqualTo("1.49 TB");
  }
}
