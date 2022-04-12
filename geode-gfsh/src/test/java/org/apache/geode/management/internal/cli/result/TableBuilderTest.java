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
package org.apache.geode.management.internal.cli.result;

import static java.util.Arrays.asList;
import static org.apache.geode.internal.util.ArrayUtils.fill;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.internal.cli.GfshParser;

public class TableBuilderTest {

  private static final int SCREEN_WIDTH = 40;

  private Screen screen;
  private TableBuilder tableBuilder;

  @Before
  public void setUp() {
    screen = mock(Screen.class);
    tableBuilder = spy(new TableBuilder());

    when(screen.getScreenWidth()).thenReturn(SCREEN_WIDTH);
    when(screen.shouldTrimColumns()).thenReturn(true);
  }

  /**
   * Test Variations table-wide separator true false
   */
  @Test
  public void testSanity() {
    var table = createTableStructure(3, "|", fill(new String[3], "Field"));
    var rowGroup = table.getLastRowGroup();
    var row = rowGroup.newRow();
    row
        .newLeftCol("1")
        .newLeftCol("1")
        .newLeftCol("1");

    var result = validateTable(table, true);

    assertThat(result)
        .hasSize(4)
        .containsExactly(
            "",
            "Field0|Field1|Field2",
            "------|------|------",
            "1     |1     |1");
  }

  @Test
  public void testLastColumnTruncated() {
    var table = createTableStructure(4, "|", fill(new String[4], "Field"));
    var rowGroup = table.getLastRowGroup();
    var row = rowGroup.newRow();
    row
        .newLeftCol("1")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-123456789-12345");

    var result = validateTable(table, true);

    assertThat(result)
        .hasSize(4)
        .containsExactly(
            "",
            "Field0|  Field1  |  Field2  |Field3",
            "------|----------|----------|-----------",
            "1     |123456789-|123456789-|123456789..");
  }

  @Test
  public void testLongestColumnFirstTruncated() {
    var table = createTableStructure(4, "|", fill(new String[4], "Field"));
    var rowGroup = table.getLastRowGroup();
    var row = rowGroup.newRow();
    row
        .newLeftCol("123456789-123456789-")
        .newLeftCol("123456789-12345")
        .newLeftCol("123456789-")
        .newLeftCol("1");

    var result = validateTable(table, true);

    assertThat(result)
        .hasSize(4)
        .containsExactly(
            "",
            "Field0|    Field1     |  Field2  |Field3",
            "------|---------------|----------|------",
            "1234..|123456789-12345|123456789-|1");
  }

  @Test
  public void testMultipleColumnsTruncated() {
    var table = createTableStructure(4, "|", fill(new String[4], "Field"));
    var rowGroup = table.getLastRowGroup();
    var row = rowGroup.newRow();
    row
        .newLeftCol("1")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-123456789-123456789-")
        .newLeftCol("123456789-123456789-12345");

    var result = validateTable(table, true);

    assertThat(result)
        .hasSize(4)
        .containsExactly(
            "",
            "Field0|  Field1  |  Field2   |Field3",
            "------|----------|-----------|---------",
            "1     |123456789-|123456789..|1234567..");
  }

  @Test
  public void testMultipleColumnsTruncatedLongestFirst() {
    var table = createTableStructure(4, "|", fill(new String[4], "Field"));
    var rowGroup = table.getLastRowGroup();
    var row = rowGroup.newRow();
    row
        .newLeftCol("123456789-123456789-123456789-")
        .newLeftCol("123456789-123456789-12345")
        .newLeftCol("1")
        .newLeftCol("123456789-");

    var result = validateTable(table, true);

    assertThat(result)
        .hasSize(4)
        .containsExactly(
            "",
            "  Field0   | Field1  |Field2|Field3",
            "-----------|---------|------|----------",
            "123456789..|1234567..|1     |123456789-");
  }

  @Test
  public void testColumnsWithShortNames() {
    when(screen.getScreenWidth()).thenReturn(9);

    var table = createTableStructure(3, "|", "A", "A", "A");
    var rowGroup = table.getLastRowGroup();
    var row = rowGroup.newRow();
    row
        .newLeftCol("123")
        .newLeftCol("123")
        .newLeftCol("123");

    var result = validateTable(table, true);

    assertThat(result)
        .hasSize(4)
        .containsExactly(
            "",
            "A0|A1|A2",
            "--|--|--",
            "..|..|..");
  }

  @Test
  public void testExceptionTooSmallWidth() {
    when(screen.getScreenWidth()).thenReturn(7);

    var table = createTableStructure(3, "|", "A", "A", "A");
    var rowGroup = table.getLastRowGroup();
    var row = rowGroup.newRow();
    row
        .newLeftCol("12")
        .newLeftCol("12")
        .newLeftCol("12");

    // This should throw an exception
    assertThatThrownBy(() -> validateTable(table, true))
        .isInstanceOf(TooManyColumnsException.class);
  }

  @Test
  public void testTooLittleSpaceOnNextToLastColumn() {
    var table = createTableStructure(4, "|", fill(new String[4], "Field"));
    var rowGroup = table.getLastRowGroup();
    var row = rowGroup.newRow();
    row
        .newLeftCol("1")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-123456789-123456789-")
        .newLeftCol("123456789-");

    var result = validateTable(table, true);

    assertThat(result)
        .hasSize(4)
        .containsExactly(
            "",
            "Field0|  Field1  |  Field2   |Field3",
            "------|----------|-----------|----------",
            "1     |123456789-|123456789..|123456789-");
  }

  @Test
  public void testSeparatorWithMultipleChars() {
    var table = createTableStructure(4, "<|>", fill(new String[4], "Field"));
    var rowGroup = table.getLastRowGroup();
    var row = rowGroup.newRow();
    row
        .newLeftCol("1")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-123456789-12345");

    var result = validateTable(table, true);

    assertThat(result)
        .hasSize(4)
        .containsExactly(
            "",
            "Field0<|>  Field1  <|>  Field2  <|>Fie..",
            "------<|>----------<|>----------<|>-----",
            "1     <|>123456789-<|>123456789-<|>123..");
  }

  @Test
  public void testManyColumns() {
    var table = createTableStructure(8, "|", fill(new String[8], "Field"));
    var rowGroup = table.getLastRowGroup();
    var row = rowGroup.newRow();
    row
        .newLeftCol("123456789-")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-");

    var result = validateTable(table, true);

    assertThat(result)
        .hasSize(4)
        .containsExactly(
            "",
            "  Field0  |  Field1  |..|..|..|..|..|..",
            "----------|----------|--|--|--|--|--|--",
            "123456789-|123456789-|..|..|..|..|..|..");
  }

  @Test
  public void testDisableColumnAdjustment() {
    when(screen.shouldTrimColumns()).thenReturn(false);

    var table = createTableStructure(5, "|", fill(new String[5], "Field"));
    var rowGroup = table.getLastRowGroup();
    var row = rowGroup.newRow();
    row
        .newLeftCol("1")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-123456789-12345")
        .newLeftCol("1");

    var result = validateTable(table, false);

    assertThat(result)
        .hasSize(4)
        .containsExactly(
            "",
            "Field0|  Field1  |  Field2  |         Field3          |Field4",
            "------|----------|----------|-------------------------|------",
            "1     |123456789-|123456789-|123456789-123456789-12345|1");
  }

  private Table createTableStructure(int columnCount, String separator, String... columnNames) {
    var resultTable = tableBuilder.newTable(screen);
    resultTable.setTabularResult(true);
    resultTable.setColumnSeparator(separator);

    resultTable.newBlankRow();
    var rowGroup = resultTable.newRowGroup();
    var row = rowGroup.newRow();

    for (var column = 0; column < columnCount; column++) {
      row.newCenterCol(columnNames[column] + column);
    }

    rowGroup.newRowSeparator('-', false);

    return resultTable;
  }

  private List<String> validateTable(Table table, boolean shouldTrim) {
    var tableAsString = table.buildTable();
    var lines = asList(tableAsString.split(GfshParser.LINE_SEPARATOR));

    for (var line : lines) {
      if (shouldTrim) {
        assertThat(line.length()).isLessThanOrEqualTo(SCREEN_WIDTH);
      } else {
        assertThat(line.length()).satisfiesAnyOf(
            length -> assertThat(length).isZero(),
            length -> assertThat(length).isGreaterThan(SCREEN_WIDTH));
      }
    }

    return lines;
  }
}
