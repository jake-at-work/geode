/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

package org.apache.geode.cache.lucene.internal.directory;

import static org.apache.geode.cache.Region.SEPARATOR;

import java.io.File;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.internal.InternalLuceneIndex;
import org.apache.geode.internal.cache.BucketNotFoundException;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class DumpDirectoryFiles implements InternalFunction {
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LogService.getLogger();
  public static final String ID = DumpDirectoryFiles.class.getSimpleName();

  @Override
  public void execute(FunctionContext context) {
    var ctx = (RegionFunctionContext) context;

    if (!(context.getArguments() instanceof String[])) {
      throw new IllegalArgumentException("Arguments should be a string array");
    }
    var args = (String[]) context.getArguments();
    if (args.length != 2) {
      throw new IllegalArgumentException("Expected 2 arguments: exportLocation, indexName");
    }

    var exportLocation = args[0];
    var indexName = args[1];

    final var region = ctx.getDataSet();
    var service = LuceneServiceProvider.get(ctx.getDataSet().getCache());
    var index =
        (InternalLuceneIndex) service.getIndex(indexName, region.getFullPath());
    if (index == null) {
      throw new IllegalStateException(
          "Index not found for region " + region + " index " + indexName);
    }

    final var repoManager = index.getRepositoryManager();
    try {
      final var repositories = repoManager.getRepositories(ctx);
      repositories.stream().forEach(repo -> {
        final var writer = repo.getWriter();
        var directory = (RegionDirectory) writer.getDirectory();
        var fs = directory.getFileSystem();

        var bucketName = index.getName() + "_" + repo.getRegion().getFullPath();
        bucketName = bucketName.replace(SEPARATOR, "_");
        var bucketDirectory = new File(exportLocation, bucketName);
        bucketDirectory.mkdirs();
        fs.export(bucketDirectory);
      });
      context.getResultSender().lastResult(null);
    } catch (BucketNotFoundException e) {
      throw new FunctionException(e);
    }
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean optimizeForWrite() {
    return true;
  }
}
