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

import static java.lang.System.lineSeparator;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.result.model.DataResultModel;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;

public class CommandResult implements Result {

  private final ResultModel result;

  private List<String> commandOutput;
  private int commandOutputIndex;

  public CommandResult(ResultModel result) {
    this.result = result;
  }

  public ResultModel getResultData() {
    return result;
  }

  public String getType() {
    return "model";
  }

  @Override
  public Status getStatus() {
    return result.getStatus();
  }

  @Override
  public void resetToFirstLine() {
    commandOutputIndex = 0;
  }

  @Override
  public boolean hasNextLine() {
    if (commandOutput == null) {
      buildCommandOutput();
    }
    return commandOutputIndex < commandOutput.size();
  }

  @Override
  public String nextLine() {
    if (commandOutput == null) {
      buildCommandOutput();
    }
    return commandOutput.get(commandOutputIndex++);
  }

  private void buildCommandOutput() {
    commandOutputIndex = 0;
    commandOutput = new ArrayList<>();
    var resultTable = new TableBuilder().newTable();

    addSpacedRowInTable(resultTable, result.getHeader());

    var index = 0;
    var sectionSize = result.getContent().size();
    for (var section : result.getContent().values()) {
      index++;
      if (section instanceof DataResultModel) {
        buildData(resultTable, (DataResultModel) section);
      } else if (section instanceof TabularResultModel) {
        buildTabularCommandOutput(resultTable, (TabularResultModel) section);
      } else if (section instanceof InfoResultModel) {
        buildInfoOrErrorCommandOutput(resultTable, (InfoResultModel) section);
      } else {
        throw new IllegalArgumentException(
            "Unable to process output for " + section.getClass().getName());
      }
      // only add the spacer in between the sections.
      if (index < sectionSize) {
        addSpacedRowInTable(resultTable, lineSeparator());
      }
    }

    addSpacedRowInTable(resultTable, result.getFooter());

    commandOutput.addAll(resultTable.buildTableList());
  }

  private void addSpacedRowInTable(Table resultTable, String row) {
    if (StringUtils.isNotBlank(row)) {
      resultTable.newRow().newLeftCol(row);
    }
  }

  private void addRowInRowGroup(RowGroup rowGroup, String row) {
    if (StringUtils.isNotBlank(row)) {
      rowGroup.newRow().newLeftCol(row);
    }
  }

  private void buildTabularCommandOutput(Table resultTable, TabularResultModel model) {
    addSpacedRowInTable(resultTable, model.getHeader());

    resultTable.setColumnSeparator("   ");
    resultTable.setTabularResult(true);

    var rowGroup = resultTable.newRowGroup();
    buildTable(rowGroup, model);

    addSpacedRowInTable(resultTable, model.getFooter());
  }

  private void buildTable(RowGroup rowGroup, TabularResultModel model) {
    var headerRow = rowGroup.newRow();
    rowGroup.setColumnSeparator(" | ");
    rowGroup.newRowSeparator('-', false);

    var rows = model.getContent();
    if (!rows.isEmpty()) {
      // build table header first
      rows.keySet().forEach(headerRow::newCenterCol);

      // each row should have the same number of entries, so just look at the first one
      var rowCount = rows.values().iterator().next().size();
      for (var i = 0; i < rowCount; i++) {
        var oneRow = rowGroup.newRow();
        for (var column : rows.keySet()) {
          oneRow.newLeftCol(rows.get(column).get(i));
        }
      }
    }
  }

  private void buildData(Table resultTable, DataResultModel section) {
    var rowGroup = resultTable.newRowGroup();
    rowGroup.setColumnSeparator(" : ");

    addRowInRowGroup(rowGroup, section.getHeader());

    // finally process map values
    for (var entry : section.getContent().entrySet()) {
      var newRow = rowGroup.newRow();
      var key = entry.getKey();
      var values = entry.getValue().split(GfshParser.LINE_SEPARATOR);
      if (values.length == 1) {
        newRow.newLeftCol(key).newLeftCol(values[0]);
      } else {
        if (values.length != 0) { // possible when object == CliConstants.LINE_SEPARATOR
          newRow.newLeftCol(key).newLeftCol(values[0]);
          for (var i = 1; i < values.length; i++) {
            newRow = rowGroup.newRow();
            newRow.setColumnSeparator("   ");
            newRow.newLeftCol("").newLeftCol(values[i]);
          }
        } else {
          newRow.newLeftCol(key).newLeftCol("");
        }
      }
    }
    addRowInRowGroup(rowGroup, section.getFooter());
  }

  private void buildInfoOrErrorCommandOutput(Table resultTable, InfoResultModel model) {
    var rowGroup = resultTable.newRowGroup();

    addRowInRowGroup(rowGroup, model.getHeader());

    model.getContent().forEach(c -> rowGroup.newRow().newLeftCol(c));

    addRowInRowGroup(rowGroup, model.getFooter());
  }
}
