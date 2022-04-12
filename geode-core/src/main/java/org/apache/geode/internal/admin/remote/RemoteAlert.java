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
package org.apache.geode.internal.admin.remote;

import java.io.PrintWriter;
import java.text.ParseException;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.admin.Alert;
import org.apache.geode.internal.admin.GemFireVM;
import org.apache.geode.internal.logging.DateFormatter;
import org.apache.geode.internal.logging.LogWriterImpl;
import org.apache.geode.logging.internal.log4j.LogLevel;

/**
 * Implementation of the Alert interface.
 */
public class RemoteAlert implements Alert {
  private final GemFireVM manager;
  private final String connectionName;
  private final String sourceId;
  private final int level;
  private final Date date;
  private final String message;
  private final InternalDistributedMember sender;

  public RemoteAlert(GemFireVM manager, int level, Date date, String connectionName,
      String threadName, long tid, String msg, String exceptionText,
      InternalDistributedMember sender) {
    this.manager = manager;
    this.level = level;
    this.date = date;
    this.connectionName = connectionName;
    {
      var tmpSourceId = new StringBuilder();

      tmpSourceId.append(threadName);
      if (tmpSourceId.length() > 0) {
        tmpSourceId.append(' ');
      }
      tmpSourceId.append("tid=0x");
      tmpSourceId.append(Long.toHexString(tid));
      sourceId = tmpSourceId.toString();
    }
    {
      var tmpMessage = new StringBuilder();
      tmpMessage.append(msg);
      if (tmpMessage.length() > 0) {
        tmpMessage.append('\n');
      }
      tmpMessage.append(exceptionText);
      message = tmpMessage.toString();
    }
    this.sender = sender;
  }

  @Override
  public int getLevel() {
    return level;
  }

  @Override
  public GemFireVM getGemFireVM() {
    return manager;
  }

  @Override
  public String getConnectionName() {
    return connectionName;
  }

  @Override
  public String getSourceId() {
    return sourceId;
  }

  @Override
  public String getMessage() {
    return message;
  }

  @Override
  public Date getDate() {
    return date;
  }

  /**
   * Returns a InternalDistributedMember instance representing a member that is sending (or has
   * sent) this alert. Could be <code>null</code>.
   *
   * @return the InternalDistributedMember instance representing a member that is sending/has sent
   *         this alert
   *
   * @since GemFire 6.5
   */
  @Override
  public InternalDistributedMember getSender() {
    return sender;
  }

  /**
   * Converts the String return by an invocation of {@link #toString} into an <code>Alert</code>.
   */
  public static Alert fromString(String s) {
    var firstBracket = s.indexOf('[');
    var lastBracket = s.indexOf(']');

    final var message = s.substring(lastBracket + 1).trim();

    var stamp = s.substring(firstBracket, lastBracket);
    var st = new StringTokenizer(stamp, "[ ");

    final var level = LogLevel.getLogWriterLevel(st.nextToken());

    var sb = new StringBuilder();
    sb.append(st.nextToken());
    sb.append(" ");
    sb.append(st.nextToken());
    sb.append(" ");
    sb.append(st.nextToken());

    final var timeFormatter = DateFormatter.createDateFormat();
    final Date date;
    try {
      date = timeFormatter.parse(sb.toString());

    } catch (ParseException ex) {
      throw new IllegalArgumentException(
          String.format("Invalidate timestamp: %s", sb));
    }

    // Assume that the connection name is only one token...
    final var connectionName = st.nextToken();

    sb = new StringBuilder();
    while (st.hasMoreTokens()) {
      sb.append(st.nextToken());
      sb.append(" ");
    }
    final var sourceId = sb.toString().trim();

    Assert.assertTrue(!st.hasMoreTokens());

    return new Alert() {
      @Override
      public int getLevel() {
        return level;
      }

      @Override
      public GemFireVM getGemFireVM() {
        return null;
      }

      @Override
      public String getConnectionName() {
        return connectionName;
      }

      @Override
      public String getSourceId() {
        return sourceId;
      }

      @Override
      public String getMessage() {
        return message;
      }

      @Override
      public Date getDate() {
        return date;
      }

      @Override
      public InternalDistributedMember getSender() {
        /* Not implemented, currently this is used only for testing purpose */
        return null;
      }
    };
  }

  @Override
  public String toString() {
    final var timeFormatter = DateFormatter.createDateFormat();
    var sw = new java.io.StringWriter();
    var pw = new PrintWriter(sw);


    pw.print('[');
    pw.print(LogWriterImpl.levelToString(level));
    pw.print(' ');
    pw.print(timeFormatter.format(date));
    pw.print(' ');
    pw.print(connectionName);
    pw.print(' ');
    pw.print(sourceId);
    pw.print("] ");
    pw.print(message);

    pw.close();
    try {
      sw.close();
    } catch (java.io.IOException ignore) {
    }
    return sw.toString();
  }
}
