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

package org.apache.geode.internal.statistics.platform;

import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import oshi.SystemInfo;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.StatisticsType;
import org.apache.geode.internal.statistics.SuppliableStatistics;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class OshiStatisticsBenchmark {

  private final int pid = new SystemInfo().getOperatingSystem().getProcessId();
  private final SuppliableStatistics noopStatistics = new NoopStatistics();

  @Setup
  public void setup() {
    OshiStatistics.init();
  }

  @Benchmark
  public void noop() {

  }

  @Benchmark
  public void refreshProcess() {
    OshiStatistics.refreshProcess(pid, noopStatistics);
  }

  @Benchmark
  public void refreshSystem() {
    OshiStatistics.refreshSystem(noopStatistics);
  }

  private static class NoopStatistics implements SuppliableStatistics {
    @Override
    public int updateSuppliedValues() {
      return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public int nameToId(final String name) {
      return 0;
    }

    @Override
    public StatisticDescriptor nameToDescriptor(final String name) {
      return null;
    }

    @Override
    public long getUniqueId() {
      return 0;
    }

    @Override
    public StatisticsType getType() {
      return null;
    }

    @Override
    public String getTextId() {
      return null;
    }

    @Override
    public long getNumericId() {
      return 0;
    }

    @Override
    public boolean isAtomic() {
      return false;
    }

    @Override
    public boolean isClosed() {
      return false;
    }

    @Override
    public void setInt(final int id, final int value) {

    }

    @Override
    public void setInt(final String name, final int value) {

    }

    @Override
    public void setInt(final StatisticDescriptor descriptor, final int value) {

    }

    @Override
    public void setLong(final int id, final long value) {

    }

    @Override
    public void setLong(final StatisticDescriptor descriptor, final long value) {

    }

    @Override
    public void setLong(final String name, final long value) {

    }

    @Override
    public void setDouble(final int id, final double value) {

    }

    @Override
    public void setDouble(final StatisticDescriptor descriptor, final double value) {

    }

    @Override
    public void setDouble(final String name, final double value) {

    }

    @Override
    public int getInt(final int id) {
      return 0;
    }

    @Override
    public int getInt(final StatisticDescriptor descriptor) {
      return 0;
    }

    @Override
    public int getInt(final String name) {
      return 0;
    }

    @Override
    public long getLong(final int id) {
      return 0;
    }

    @Override
    public long getLong(final StatisticDescriptor descriptor) {
      return 0;
    }

    @Override
    public long getLong(final String name) {
      return 0;
    }

    @Override
    public double getDouble(final int id) {
      return 0;
    }

    @Override
    public double getDouble(final StatisticDescriptor descriptor) {
      return 0;
    }

    @Override
    public double getDouble(final String name) {
      return 0;
    }

    @Override
    public Number get(final StatisticDescriptor descriptor) {
      return null;
    }

    @Override
    public Number get(final String name) {
      return null;
    }

    @Override
    public long getRawBits(final StatisticDescriptor descriptor) {
      return 0;
    }

    @Override
    public long getRawBits(final String name) {
      return 0;
    }

    @Override
    public void incInt(final int id, final int delta) {

    }

    @Override
    public void incInt(final StatisticDescriptor descriptor, final int delta) {

    }

    @Override
    public void incInt(final String name, final int delta) {

    }

    @Override
    public void incLong(final int id, final long delta) {

    }

    @Override
    public void incLong(final StatisticDescriptor descriptor, final long delta) {

    }

    @Override
    public void incLong(final String name, final long delta) {

    }

    @Override
    public void incDouble(final int id, final double delta) {

    }

    @Override
    public void incDouble(final StatisticDescriptor descriptor, final double delta) {

    }

    @Override
    public void incDouble(final String name, final double delta) {

    }

    @Override
    public IntSupplier setIntSupplier(final int id, final IntSupplier supplier) {
      return null;
    }

    @Override
    public IntSupplier setIntSupplier(final String name, final IntSupplier supplier) {
      return null;
    }

    @Override
    public IntSupplier setIntSupplier(final StatisticDescriptor descriptor,
                                      final IntSupplier supplier) {
      return null;
    }

    @Override
    public LongSupplier setLongSupplier(final int id, final LongSupplier supplier) {
      return null;
    }

    @Override
    public LongSupplier setLongSupplier(final String name, final LongSupplier supplier) {
      return null;
    }

    @Override
    public LongSupplier setLongSupplier(final StatisticDescriptor descriptor,
                                        final LongSupplier supplier) {
      return null;
    }

    @Override
    public DoubleSupplier setDoubleSupplier(final int id, final DoubleSupplier supplier) {
      return null;
    }

    @Override
    public DoubleSupplier setDoubleSupplier(final String name, final DoubleSupplier supplier) {
      return null;
    }

    @Override
    public DoubleSupplier setDoubleSupplier(final StatisticDescriptor descriptor,
                                            final DoubleSupplier supplier) {
      return null;
    }
  }
}
