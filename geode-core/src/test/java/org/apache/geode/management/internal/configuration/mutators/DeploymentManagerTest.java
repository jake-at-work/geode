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

package org.apache.geode.management.internal.configuration.mutators;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class DeploymentManagerTest {

  private final Function<String, Deployment> function;

  public DeploymentManagerTest(Function<String, Deployment> function) {
    this.function = function;
  }

  @Test
  public void listWithNullJarNameReturnsAllDeployedJarsForGroup() {

    Set<Deployment> configuredDeployments = new HashSet<>(asList(
        new Deployment("jar1.jar", "deployedBy1", "deployedTime1"),
        new Deployment("jar2.jar", "deployedBy2", "deployedTime2"),
        new Deployment("jar3.jar", "deployedBy3", "deployedTime3")));

    var configuration = mock(Configuration.class);
    when(configuration.getDeployments()).thenReturn(configuredDeployments);

    var persistenceService =
        mock(InternalConfigurationPersistenceService.class);
    when(persistenceService.getConfiguration(any())).thenReturn(configuration);

    var manager = new DeploymentManager(persistenceService);

    var filter = function.apply(null);

    var result = manager.list(filter, "some-group");

    assertThat(result)
        .containsExactlyInAnyOrderElementsOf(configuredDeployments);
  }

  @Test
  public void listWithJarNameReturnsSingletonListConfiguredDeploymentForThatJar() {
    var requestedJarFile = "jar2.jar";
    var expectedDeployment =
        new Deployment(requestedJarFile, "deployedBy2", "deployedTime2");

    Set<Deployment> configuredJarNames = new HashSet<>(asList(
        new Deployment("jar1.jar", "deployedBy1", "deployedTime1"),
        expectedDeployment,
        new Deployment("jar3.jar", "deployedBy3", "deployedTime3")));

    var configuration = mock(Configuration.class);
    when(configuration.getDeployments()).thenReturn(configuredJarNames);

    var persistenceService =
        mock(InternalConfigurationPersistenceService.class);
    when(persistenceService.getConfiguration(any())).thenReturn(configuration);

    var manager = new DeploymentManager(persistenceService);

    var filter = function.apply(requestedJarFile);

    var result = manager.list(filter, "some-group");

    assertThat(result).containsExactly(expectedDeployment);
  }

  @Test
  public void listWithJarNameReturnsEmptyListIfRequestedJarNotDeployed() {
    var configuration = mock(Configuration.class);
    when(configuration.getDeployments()).thenReturn(emptySet());

    var persistenceService =
        mock(InternalConfigurationPersistenceService.class);
    when(persistenceService.getConfiguration(any())).thenReturn(configuration);

    var manager = new DeploymentManager(persistenceService);

    var filter = function.apply("jarFileThatHasNotBeenDeployed.jar");

    var result = manager.list(filter, "some-group");

    assertThat(result).isEmpty();
  }

  @Test
  public void listNonExistentGroup() {
    var persistenceService =
        mock(InternalConfigurationPersistenceService.class);
    when(persistenceService.getConfiguration("some-group")).thenReturn(null);

    var manager = new DeploymentManager(persistenceService);

    var filter = function.apply(null);

    var result = manager.list(filter, "some-group");

    assertThat(result).isEmpty();
  }

  @Parameters
  public static List<Function<String, Deployment>> consumers() {
    return Arrays.asList(name -> {
      var deployment = new Deployment();
      deployment.setFileName(name);
      return deployment;
    }, name -> {
      var deployment = new Deployment();
      deployment.setFileName(name);
      return deployment;
    });
  }
}
