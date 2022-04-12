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

package org.apache.geode.tools.pulse.internal;

import java.io.IOException;
import java.util.Properties;
import java.util.ResourceBundle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

@Component
public class ClassPathPropertiesFileLoader implements PropertiesFileLoader {
  private static final Logger logger = LogManager.getLogger();

  @Override
  public Properties loadProperties(String propertyFile, ResourceBundle resourceBundle) {
    final var properties = new Properties();
    try (final var stream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(propertyFile)) {
      logger.info(propertyFile + " " + resourceBundle.getString("LOG_MSG_FILE_FOUND"));
      properties.load(stream);
    } catch (IOException e) {
      logger.error(resourceBundle.getString("LOG_MSG_EXCEPTION_LOADING_PROPERTIES_FILE"), e);
    }

    return properties;
  }
}
