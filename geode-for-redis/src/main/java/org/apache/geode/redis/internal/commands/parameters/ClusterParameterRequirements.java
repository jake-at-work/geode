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

package org.apache.geode.redis.internal.commands.parameters;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_UNKNOWN_CLUSTER_SUBCOMMAND;

import java.util.function.Consumer;

import org.apache.geode.redis.internal.commands.Command;

public class ClusterParameterRequirements {

  public static Consumer<Command> checkParameters() {
    return command -> {
      var numberOfArguments = command.getProcessedCommand().size();

      if (numberOfArguments < 2) {
        throw new RedisParametersMismatchException(command.wrongNumberOfArgumentsErrorMessage());
      } else if (numberOfArguments > 3) {
        throw new RedisParametersMismatchException(
            String.format(ERROR_UNKNOWN_CLUSTER_SUBCOMMAND, command.getStringKey()));
      }
    };
  }

}
