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
package org.apache.geode.management.internal;

import java.io.InvalidObjectException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;
import java.util.SortedMap;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

/**
 * Table type converter
 *
 *
 */
public class TableConverter extends OpenTypeConverter {
  TableConverter(Type targetType, boolean sortedMap, TabularType tabularType,
      OpenTypeConverter keyConverter, OpenTypeConverter valueConverter) {
    super(targetType, tabularType, TabularData.class);
    this.sortedMap = sortedMap;
    this.keyConverter = keyConverter;
    this.valueConverter = valueConverter;
  }

  @Override
  Object toNonNullOpenValue(Object value) throws OpenDataException {
    final var valueMap = (Map<Object, Object>) value;
    if (valueMap instanceof SortedMap) {
      var comparator = ((SortedMap) valueMap).comparator();
      if (comparator != null) {
        final var msg = "Cannot convert SortedMap with non-null comparator: " + comparator;
        var iae = new IllegalArgumentException(msg);
        var ode = new OpenDataException(msg);
        ode.initCause(iae);
        throw ode;
      }
    }
    final var tabularType = (TabularType) getOpenType();
    final TabularData table = new TabularDataSupport(tabularType);
    final var rowType = tabularType.getRowType();
    for (Map.Entry entry : valueMap.entrySet()) {
      final var openKey = keyConverter.toOpenValue(entry.getKey());
      final var openValue = valueConverter.toOpenValue(entry.getValue());
      final CompositeData row;
      row = new CompositeDataSupport(rowType, keyValueArray, new Object[] {openKey, openValue});
      table.put(row);
    }
    return table;
  }

  @Override
  public Object fromNonNullOpenValue(Object openValue) throws InvalidObjectException {
    final var table = (TabularData) openValue;
    final var rows = (Collection<CompositeData>) table.values();
    final var valueMap =
        sortedMap ? OpenTypeUtil.newSortedMap() : OpenTypeUtil.newMap();
    for (var row : rows) {
      final var key = keyConverter.fromOpenValue(row.get("key"));
      final var value = valueConverter.fromOpenValue(row.get("value"));
      if (valueMap.put(key, value) != null) {
        final var msg = "Duplicate entry in TabularData: key=" + key;
        throw new InvalidObjectException(msg);
      }
    }
    return valueMap;
  }

  @Override
  void checkReconstructible() throws InvalidObjectException {
    keyConverter.checkReconstructible();
    valueConverter.checkReconstructible();
  }

  private final boolean sortedMap;
  private final OpenTypeConverter keyConverter;
  private final OpenTypeConverter valueConverter;
}
