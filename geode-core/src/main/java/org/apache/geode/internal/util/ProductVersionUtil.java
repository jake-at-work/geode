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

package org.apache.geode.internal.util;

import static java.lang.System.lineSeparator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.logging.log4j.util.TriConsumer;
import org.jetbrains.annotations.NotNull;

import org.apache.geode.internal.GeodeVersion;
import org.apache.geode.internal.version.ComponentVersion;
import org.apache.geode.internal.version.DistributionVersion;

public class ProductVersionUtil {

  private static final String LINE = "----------------------------------------";
  private static final String KEY_VALUE_SEPARATOR = ": ";

  public static @NotNull DistributionVersion getDistributionVersion() {
    final ServiceLoader<DistributionVersion> loader = ServiceLoader.load(DistributionVersion.class);
    final Iterator<DistributionVersion> loaderIter = loader.iterator();
    if (loaderIter.hasNext()) {
      return loaderIter.next();
    }
    return new GeodeVersion();
  }

  public static @NotNull Iterable<ComponentVersion> getComponentVersions() {
    return ServiceLoader.load(ComponentVersion.class);
  }

  public static <T extends Appendable> @NotNull T appendFullVersion(final @NotNull T appendable)
      throws IOException {
    buildFullVersion(
        (n) -> appendable.append(LINE).append(lineSeparator()).append(n).append(lineSeparator())
            .append(LINE).append(lineSeparator()),
        (c, k, v) -> appendable.append(k).append(KEY_VALUE_SEPARATOR).append(v)
            .append(lineSeparator()),
        (c, n) -> appendable.append(LINE).append(lineSeparator()));

    return appendable;
  }

  public static @NotNull String getFullVersion() {
    try {
      return appendFullVersion(new StringBuilder()).toString();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }


  @FunctionalInterface
  public interface BeginComponent<Context> extends
      Function<@NotNull String, Context> {
    @Override
    default Context apply(final @NotNull String name) {
      try {
        return applyThrows(name);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    Context applyThrows(@NotNull String name) throws Exception;
  }

  @FunctionalInterface
  public interface ComponentDetail<Context> extends
      TriConsumer<Context, @NotNull String, @NotNull String> {
    @Override
    default void accept(final Context context, final @NotNull String key,
        final @NotNull String value) {
      try {
        acceptThrows(context, key, value);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    void acceptThrows(Context context, @NotNull String key, @NotNull String value)
        throws IOException;
  }

  @FunctionalInterface
  public interface EndComponent<Context> extends
      BiConsumer<Context, @NotNull String> {
    @Override
    default void accept(final Context context, final @NotNull String name) {
      try {
        acceptThrows(context, name);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    void acceptThrows(Context context, @NotNull String name) throws IOException;
  }

  public static <Context> void buildFullVersion(
      final @NotNull BeginComponent<Context> beginComponent,
      final @NotNull ComponentDetail<Context> detail,
      final @NotNull EndComponent<Context> endComponent) {
    for (final ComponentVersion version : getComponentVersions()) {
      final Context context = beginComponent.apply(version.getName());
      for (final Map.Entry<String, String> entry : version.getDetails().entrySet()) {
        detail.accept(context, entry.getKey(), entry.getValue());
      }
      endComponent.accept(context, version.getName());
    }
  }
}
