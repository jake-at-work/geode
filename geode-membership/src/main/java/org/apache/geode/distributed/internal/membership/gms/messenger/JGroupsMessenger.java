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
package org.apache.geode.distributed.internal.membership.gms.messenger;

import static org.apache.geode.distributed.internal.membership.gms.GMSUtil.replaceStrings;
import static org.apache.geode.distributed.internal.membership.gms.messages.AbstractGMSMessage.ALL_RECIPIENTS;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.FIND_COORDINATOR_REQ;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.FIND_COORDINATOR_RESP;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.JOIN_REQUEST;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.JOIN_RESPONSE;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.logging.log4j.Logger;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.Message.Flag;
import org.jgroups.Message.TransientFlag;
import org.jgroups.Receiver;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.UDP;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Digest;
import org.jgroups.util.UUID;

import org.apache.geode.distributed.internal.membership.api.CacheOperationMessageMarker;
import org.apache.geode.distributed.internal.membership.api.MemberDisconnectedException;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.MemberShunnedException;
import org.apache.geode.distributed.internal.membership.api.MemberStartupException;
import org.apache.geode.distributed.internal.membership.api.MembershipClosedException;
import org.apache.geode.distributed.internal.membership.api.MembershipConfig;
import org.apache.geode.distributed.internal.membership.api.MembershipConfigurationException;
import org.apache.geode.distributed.internal.membership.api.Message;
import org.apache.geode.distributed.internal.membership.gms.GMSMemberData;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.distributed.internal.membership.gms.GMSUtil;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.interfaces.HealthMonitor;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Manager;
import org.apache.geode.distributed.internal.membership.gms.interfaces.MessageHandler;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Messenger;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorRequest;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorResponse;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinResponseMessage;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.serialization.BufferDataOutputStream;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.StaticSerialization;
import org.apache.geode.internal.serialization.VersionedDataInputStream;
import org.apache.geode.internal.serialization.Versioning;
import org.apache.geode.internal.serialization.VersioningIO;
import org.apache.geode.logging.internal.OSProcess;
import org.apache.geode.util.internal.GeodeGlossary;


/**
 * JGroupsMessenger performs all Membership messaging using a JGroups UDP stack.
 * It uses one of two JGroups configuration files stored in the Geode jar file to
 * configure a simple JGroups stack depending on whether or not multicast is enabled.<br>
 * JGroupsMessenger can send UDP messages using either reliable or non-reliable
 * protocols.<br>
 * Messages must implement the membership Message interface and must be serializable
 * via the serializer/deserializer installed in a Membership.
 */
@SuppressWarnings("StatementWithEmptyBody")
public class JGroupsMessenger<ID extends MemberIdentifier> implements Messenger<ID> {

  private static final Logger logger = Services.getLogger();

  /**
   * The location (in the product) of the non-mcast Jgroups config file.
   */
  private static final String DEFAULT_JGROUPS_TCP_CONFIG =
      "org/apache/geode/distributed/internal/membership/gms/messenger/jgroups-config.xml";

  /**
   * The location (in the product) of the mcast Jgroups config file.
   */
  private static final String JGROUPS_MCAST_CONFIG_FILE_NAME =
      "org/apache/geode/distributed/internal/membership/gms/messenger/jgroups-mcast.xml";

  /** JG magic numbers for types added to the JG ClassConfigurator */
  private static final short JGROUPS_TYPE_JGADDRESS = 2000;
  private static final short JGROUPS_PROTOCOL_TRANSPORT = 1000;

  protected String jgStackConfig;

  JChannel myChannel;
  ID localAddress;
  JGAddress jgAddress;
  private Services<ID> services;

  public JGroupsMessenger() {}

  /** handlers that receive certain classes of messages instead of the Manager */
  private final Map<Class<?>, MessageHandler<?>> handlers = new ConcurrentHashMap<>();

  private volatile GMSMembershipView<ID> view;

  protected final GMSPingPonger pingPonger = new GMSPingPonger();

  protected final AtomicLong pongsReceived = new AtomicLong(0);

  /** tracks multicast messages that have been scheduled for processing */
  protected final Map<ID, MessageTracker> scheduledMcastSeqnos = new HashMap<>();

  protected short nackack2HeaderId;

  /**
   * A set that contains addresses that we have logged JGroups IOExceptions for in the current
   * membership view and possibly initiated suspect processing. This reduces the amount of suspect
   * processing initiated by IOExceptions and the amount of exceptions logged
   */
  private final Set<Address> addressesWithIoExceptionsProcessed =
      Collections.synchronizedSet(new HashSet<>());

  static {
    // register classes that we've added to jgroups that are put on the wire
    // or need a header ID
    ClassConfigurator.add(JGROUPS_TYPE_JGADDRESS, JGAddress.class);
    ClassConfigurator.addProtocol(JGROUPS_PROTOCOL_TRANSPORT, Transport.class);
  }

  private GMSEncrypt<ID> encrypt;

  /**
   * During reconnect a QuorumChecker holds the JGroups channel and responds to Ping
   * and Pong messages but also queues any messages it doesn't recognize. These need
   * to be delivered to handlers after membership services have been rebuilt.
   */
  private Queue<org.jgroups.Message> queuedMessagesFromReconnect;

  /**
   * The JGroupsReceiver is handed messages by the JGroups Channel. It is responsible
   * for deserializating and dispatching those messages to the appropriate handler
   */
  protected JGroupsReceiver jgroupsReceiver;

  public static void setChannelReceiver(JChannel channel, Receiver r) {
    try {
      // Channel.setReceiver() will issue a warning if we try to set a new receiver
      // and the channel already has one. Rather than set the receiver to null &
      // then establish a new one we use reflection to set the channel receiver. See GEODE-7220
      var receiver = Channel.class.getDeclaredField("receiver");
      receiver.setAccessible(true);
      receiver.set(channel, r);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new IllegalStateException("unable to establish a JGroups receiver", e);
    }
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
  public void init(Services<ID> s) throws MembershipConfigurationException {
    services = s;

    var config = services.getConfig();

    var enableNetworkPartitionDetection = config.isNetworkPartitionDetectionEnabled();
    System.setProperty("jgroups.resolve_dns", String.valueOf(!enableNetworkPartitionDetection));

    InputStream is = null;

    String r;
    if (config.isMulticastEnabled()) {
      r = JGROUPS_MCAST_CONFIG_FILE_NAME;
    } else {
      r = DEFAULT_JGROUPS_TCP_CONFIG;
    }
    var contextClassLoader = Thread.currentThread().getContextClassLoader();
    if (contextClassLoader != null) {
      is = contextClassLoader.getResourceAsStream(r);
    }
    if (is == null) {
      is = getClass().getResourceAsStream(r);
    }
    if (is == null) {
      is = ClassLoader.getSystemResourceAsStream(r);
    }
    if (is == null) {
      throw new MembershipConfigurationException(
          String.format("Cannot find %s", r));
    }

    String properties;
    try {
      var sb = new StringBuilder(3000);
      BufferedReader br;
      br = new BufferedReader(new InputStreamReader(is, StandardCharsets.US_ASCII));
      String input;
      while ((input = br.readLine()) != null) {
        sb.append(input);
      }
      br.close();
      properties = sb.toString();
    } catch (Exception ex) {
      throw new MembershipConfigurationException(
          "An Exception was thrown while reading JGroups config.",
          ex);
    }

    if (properties.startsWith("<!--")) {
      var commentEnd = properties.indexOf("-->");
      properties = properties.substring(commentEnd + 3);
    }

    if (config.isMulticastEnabled()) {
      properties = replaceStrings(properties, "MCAST_PORT",
          String.valueOf(config.getMcastPort()));
      properties =
          replaceStrings(properties, "MCAST_ADDRESS", config.getMcastAddress());
      properties = replaceStrings(properties, "MCAST_TTL", String.valueOf(config.getMcastTtl()));
      properties = replaceStrings(properties, "MCAST_SEND_BUFFER_SIZE",
          String.valueOf(config.getMcastSendBufferSize()));
      properties = replaceStrings(properties, "MCAST_RECV_BUFFER_SIZE",
          String.valueOf(config.getMcastRecvBufferSize()));
      properties = replaceStrings(properties, "MCAST_RETRANSMIT_INTERVAL", "" + Integer
          .getInteger(GeodeGlossary.GEMFIRE_PREFIX + "mcast-retransmit-interval", 500));
      properties = replaceStrings(properties, "RETRANSMIT_LIMIT",
          String.valueOf(config.getUdpFragmentSize() - 256));
    }

    if (config.isMulticastEnabled() || config.getDisableTcp()
        || (config.getUdpRecvBufferSize() != MembershipConfig.DEFAULT_UDP_RECV_BUFFER_SIZE)) {
      properties =
          replaceStrings(properties, "UDP_RECV_BUFFER_SIZE", "" + config.getUdpRecvBufferSize());
    } else {
      properties = replaceStrings(properties, "UDP_RECV_BUFFER_SIZE",
          "" + MembershipConfig.DEFAULT_UDP_RECV_BUFFER_SIZE_REDUCED);
    }
    properties =
        replaceStrings(properties, "UDP_SEND_BUFFER_SIZE", "" + config.getUdpSendBufferSize());

    var str = config.getBindAddress();
    // JGroups UDP protocol requires a bind address
    if (str == null || str.length() == 0) {
      try {
        str = LocalHostUtil.getLocalHost().getHostAddress();
      } catch (UnknownHostException e) {
        throw new MembershipConfigurationException(e.getMessage(), e);
      }
    }
    properties = replaceStrings(properties, "BIND_ADDR_SETTING", "bind_addr=\"" + str + "\"");

    int port = Integer.getInteger(GeodeGlossary.GEMFIRE_PREFIX + "jg-bind-port", 0);
    if (port != 0) {
      properties = replaceStrings(properties, "MEMBERSHIP_PORT_RANGE_START", "" + port);
      properties = replaceStrings(properties, "MEMBERSHIP_PORT_RANGE", "" + 0);
    } else {
      var ports = config.getMembershipPortRange();
      properties = replaceStrings(properties, "MEMBERSHIP_PORT_RANGE_START", "" + ports[0]);
      properties = replaceStrings(properties, "MEMBERSHIP_PORT_RANGE", "" + (ports[1] - ports[0]));
    }

    properties = replaceStrings(properties, "UDP_FRAGMENT_SIZE", "" + config.getUdpFragmentSize());

    properties = replaceStrings(properties, "FC_MAX_CREDITS",
        "" + config.getMcastByteAllowance());
    properties = replaceStrings(properties, "FC_THRESHOLD",
        "" + config.getMcastRechargeThreshold());
    properties = replaceStrings(properties, "FC_MAX_BLOCK",
        "" + config.getMcastRechargeBlockMs());

    jgStackConfig = properties;

    if (!config.getSecurityUDPDHAlgo().isEmpty()) {
      try {
        encrypt = new GMSEncrypt<>(services, config.getSecurityUDPDHAlgo());
        logger.info("Initializing GMSEncrypt ");
      } catch (Exception e) {
        throw new MembershipConfigurationException("problem initializing encryption protocol", e);
      }
    }
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
  public void start() throws MemberStartupException {
    // create the configuration XML string for JGroups
    var properties = jgStackConfig;

    var start = System.currentTimeMillis();

    // start the jgroups channel and establish the membership ID
    var reconnecting = false;
    try {
      var oldDSMembershipInfo = services.getConfig().getOldMembershipInfo();
      if (oldDSMembershipInfo != null) {
        if (logger.isDebugEnabled()) {
          logger.debug("Reusing JGroups channel from previous system", properties);
        }
        var oldInfo = (MembershipInformationImpl) oldDSMembershipInfo;
        myChannel = oldInfo.getChannel();
        queuedMessagesFromReconnect = oldInfo.getQueuedMessages();
        encrypt = oldInfo.getEncrypt();

        // scrub the old channel
        var vid = new ViewId(new JGAddress(), 0);
        List<Address> members = new ArrayList<>();
        members.add(new UUID(0, 0));// TODO open a JGroups JIRA for GEODE-3034
        var jgv = new View(vid, members);
        myChannel.down(new Event(Event.VIEW_CHANGE, jgv));
        // attempt to establish a new UUID in the jgroups channel so the member address will be
        // different
        try {
          var setAddressMethod = JChannel.class.getDeclaredMethod("setAddress");
          setAddressMethod.setAccessible(true);
          setAddressMethod.invoke(myChannel);
        } catch (SecurityException | NoSuchMethodException e) {
          logger.warn("Unable to establish a new JGroups address.  "
              + "My address will be exactly the same as last time. Exception={}",
              e.getMessage());
        }
        reconnecting = true;
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug("JGroups configuration: {}", properties);
        }

        checkForIPv6();
        InputStream is = new ByteArrayInputStream(properties.getBytes(StandardCharsets.UTF_8));
        myChannel = new JChannel(is);
      }
    } catch (Exception e) {
      throw new MembershipConfigurationException("unable to create jgroups channel", e);
    }

    // give the stats to the jchannel statistics recorder
    var sr =
        (StatRecorder<ID>) myChannel.getProtocolStack().findProtocol(StatRecorder.class);
    if (sr != null) {
      sr.setServices(services);
    }

    var transport = (Transport<ID>) myChannel.getProtocolStack().getTransport();
    transport.setMessenger(this);

    nackack2HeaderId = ClassConfigurator.getProtocolId(NAKACK2.class);

    try {
      jgroupsReceiver = new JGroupsReceiver();
      try {
        setChannelReceiver(myChannel, jgroupsReceiver);
      } catch (IllegalStateException e) {
        throw new MemberStartupException("problem initializing JGroups", e);
      }
      if (!reconnecting) {
        myChannel.connect("AG"); // Apache Geode
      }
    } catch (Exception e) {
      myChannel.close();
      throw new MemberStartupException("unable to create jgroups channel", e);
    }

    establishLocalAddress();

    logger.info("JGroups channel {} (took {}ms)", (reconnecting ? "reinitialized" : "created"),
        System.currentTimeMillis() - start);

  }

  /**
   * JGroups picks an IPv6 address if preferIPv4Stack is false or not set and preferIPv6Addresses is
   * not set or is true. We want it to use an IPv4 address for a dual-IP stack so that both IPv4 and
   * IPv6 messaging work
   */
  private void checkForIPv6() throws Exception {
    var preferIpV6Addr = Boolean.getBoolean("java.net.preferIPv6Addresses");
    if (!preferIpV6Addr) {
      if (logger.isDebugEnabled()) {
        logger
            .debug("forcing JGroups to think IPv4 is being used so it will choose an IPv4 address");
      }
      var m = org.jgroups.util.Util.class.getDeclaredField("ip_stack_type");
      m.setAccessible(true);
      m.set(null, org.jgroups.util.StackType.IPv4);
    }
  }

  @Override
  public void started() throws MemberStartupException {
    if (queuedMessagesFromReconnect != null) {
      logger.info("Delivering {} messages queued by quorum checker",
          queuedMessagesFromReconnect.size());
      for (var message : queuedMessagesFromReconnect) {
        jgroupsReceiver.receive(message, true);
      }
      queuedMessagesFromReconnect.clear();
      queuedMessagesFromReconnect = null;
    }
  }

  @Override
  public void stop() {
    if (myChannel != null) {
      if ((services.isShutdownDueToForcedDisconnect() && services.isAutoReconnectEnabled())
          || services.getManager().isReconnectingDS()) {
        // leave the channel open for reconnect attempts
      } else {
        myChannel.close();
      }
    }
  }

  @Override
  public void stopped() {}

  @Override
  public void memberSuspected(ID initiator,
      ID suspect, String reason) {}

  @Override
  public void installView(GMSMembershipView<ID> v) {
    view = v;

    if (jgAddress.getVmViewId() < 0) {
      jgAddress.setVmViewId(localAddress.getVmViewId());
    }
    var mbrs = v.getMembers().stream().map(JGAddress::new).collect(Collectors.toList());
    var vid = new ViewId(new JGAddress(v.getCoordinator()), v.getViewId());
    var jgv = new View(vid, new ArrayList<>(mbrs));
    if (logger.isTraceEnabled()) {
      logger.trace("installing view into JGroups stack: {}", jgv);
    }
    myChannel.down(new Event(Event.VIEW_CHANGE, jgv));

    addressesWithIoExceptionsProcessed.clear();
    if (encrypt != null) {
      encrypt.installView(v);
    }
    synchronized (scheduledMcastSeqnos) {
      for (var mbr : v.getCrashedMembers()) {
        scheduledMcastSeqnos.remove(mbr);
      }
      for (var mbr : v.getShutdownMembers()) {
        scheduledMcastSeqnos.remove(mbr);
      }
    }
  }


  /**
   * If JGroups is unable to send a message it may mean that the network is down. If so we need to
   * initiate suspect processing on the recipient.
   * <p>
   * see Transport._send()
   */
  @SuppressWarnings("UnusedParameters")
  public void handleJGroupsIOException(IOException e, Address dest) {
    if (services.getManager().shutdownInProgress()) { // GEODE-634 - don't log IOExceptions during
                                                      // shutdown
      return;
    }
    var v = view;
    var jgMbr = (JGAddress) dest;
    if (jgMbr != null && v != null) {
      var members = v.getMembers();
      ID recipient = null;
      for (var gmsMbr : members) {
        if (jgMbr.getUUIDLsbs() == gmsMbr.getUuidLeastSignificantBits()
            && jgMbr.getUUIDMsbs() == gmsMbr.getUuidMostSignificantBits()
            && jgMbr.getVmViewId() == gmsMbr.getVmViewId()) {
          recipient = gmsMbr;
          break;
        }
      }
      if (recipient != null) {
        if (!addressesWithIoExceptionsProcessed.contains(dest)) {
          logger.warn("Unable to send message to " + recipient, e);
          addressesWithIoExceptionsProcessed.add(dest);
        }
        // If communications aren't working we need to resolve the issue quickly, so here
        // we initiate a final check. Prior to becoming open-source we did a similar check
        // using JGroups VERIFY_SUSPECT
        services.getHealthMonitor().checkIfAvailable(recipient,
            "Unable to send messages to this member via JGroups", true);
      }
    }
  }

  private void establishLocalAddress() throws MemberStartupException {
    var logicalAddress = (UUID) myChannel.getAddress();
    logicalAddress = logicalAddress.copy();

    var ipaddr = (IpAddress) myChannel.down(new Event(Event.GET_PHYSICAL_ADDRESS));

    if (ipaddr != null) {
      jgAddress = new JGAddress(logicalAddress, ipaddr);
    } else {
      var udp = (UDP) myChannel.getProtocolStack().getTransport();

      try {
        var getAddress = UDP.class.getDeclaredMethod("getPhysicalAddress");
        getAddress.setAccessible(true);
        ipaddr = (IpAddress) getAddress.invoke(udp, new Object[0]);
        jgAddress = new JGAddress(logicalAddress, ipaddr);
      } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
        throw new MemberStartupException(
            "Unable to configure JGroups channel for membership communications", e);
      }
    }

    // install the address in the JGroups channel protocols
    myChannel.down(new Event(Event.SET_LOCAL_ADDRESS, jgAddress));

    var config = services.getConfig();
    var isLocator = (config
        .getVmKind() == MemberIdentifier.LOCATOR_DM_TYPE)
        || !config.getStartLocator().isEmpty();

    // establish the DistributedSystem's address
    var hostname = config.getBindAddress();
    if (hostname == null || hostname.isEmpty()) {
      hostname = jgAddress.getInetAddress().getHostName();
    }
    var gmsMember = new GMSMemberData(jgAddress.getInetAddress(),
        hostname, jgAddress.getPort(),
        OSProcess.getId(), (byte) services.getConfig().getVmKind(),
        -1 /* directport */, -1 /* viewID */, config.getName(),
        GMSUtil.parseGroups(config.getRoles(), config.getGroups()), config.getDurableClientId(),
        config.getDurableClientTimeout(),
        config.isNetworkPartitionDetectionEnabled(), isLocator,
        KnownVersion.getCurrentVersion().ordinal(),
        jgAddress.getUUIDMsbs(), jgAddress.getUUIDLsbs(),
        (byte) (services.getConfig().getMemberWeight() & 0xff), false, null);
    localAddress = services.getMemberFactory().create(gmsMember);
    logger.info("Established local address {}", localAddress);
    services.setLocalAddress(localAddress);
  }

  @Override
  public void beSick() {}

  @Override
  public void playDead() {}

  @Override
  public void beHealthy() {}

  @Override
  public <T extends Message<ID>> void addHandler(Class<T> c, MessageHandler<T> h) {
    handlers.put(c, h);
  }

  @Override
  public boolean testMulticast(long timeout) throws InterruptedException {
    var pongsSnapshot = pongsReceived.longValue();
    JGAddress dest = null;
    try {
      // noinspection ConstantConditions
      pingPonger.sendPingMessage(myChannel, jgAddress, dest);
    } catch (Exception e) {
      logger.warn("unable to send multicast message: {}",
          (jgAddress == null ? "multicast recipients" : jgAddress), e.getMessage());
      return false;
    }
    var giveupTime = System.currentTimeMillis() + timeout;
    while (pongsReceived.longValue() == pongsSnapshot && System.currentTimeMillis() < giveupTime) {
      Thread.sleep(100);
    }
    return pongsReceived.longValue() > pongsSnapshot;
  }

  @Override
  public void getMessageState(ID target, Map<String, Long> state,
      boolean includeMulticast) {
    if (includeMulticast) {
      var nakack = (NAKACK2) myChannel.getProtocolStack().findProtocol("NAKACK2");
      if (nakack != null) {
        var seqno = nakack.getCurrentSeqno();
        state.put("JGroups.mcastState", seqno);
      }
    }
  }

  @Override
  public void waitForMessageState(ID sender, Map<String, Long> state)
      throws InterruptedException, TimeoutException {
    var seqno = state.get("JGroups.mcastState");
    if (seqno == null) {
      return;
    }
    var timeout = services.getConfig().getAckWaitThreshold() * 1000L;
    var startTime = System.currentTimeMillis();
    var warnTime = startTime + timeout;
    var quitTime = warnTime + timeout - 1000L;
    var warned = false;

    for (;;) {
      var received = "none";
      long highSeqno = 0;
      synchronized (scheduledMcastSeqnos) {
        var tracker = scheduledMcastSeqnos.get(sender);
        if (tracker == null) { // no longer in the membership view
          break;
        }
        highSeqno = tracker.get();
      }

      if (logger.isDebugEnabled()) {
        logger.debug(
            "waiting for multicast messages from {}.  Current seqno={} and expected seqno={}",
            sender, highSeqno, seqno);
      }
      if (highSeqno >= seqno) {
        break;
      }
      var now = System.currentTimeMillis();
      if (!warned && now >= warnTime) {
        warned = true;
        received = String.valueOf(highSeqno);
        logger.warn(
            "{} seconds have elapsed while waiting for multicast messages from {}.  Received {} but expecting at least {}.",
            Long.toString((warnTime - startTime) / 1000L), sender, received, seqno);
      }
      if (now >= quitTime) {
        throw new TimeoutException("Multicast operations from " + sender
            + " did not distribute within " + (now - startTime) + " milliseconds");
      }
      Thread.sleep(50);
    }
  }

  @Override
  public Set<ID> sendUnreliably(Message<ID> msg) {
    return send(msg, false);
  }

  @Override
  public Set<ID> send(Message<ID> msg) {
    return send(msg, true);
  }

  private Set<ID> send(Message<ID> msg, boolean reliably) {

    // perform the same jgroups messaging as in 8.2's GMSMembershipManager.send() method

    // BUT: when marshalling messages we need to include the version of the product and
    // localAddress at the beginning of the message. These should be used in the receiver
    // code to create a versioned input stream, read the sender address, then read the message
    // and set its sender address
    var theStats = services.getStatistics();
    var oldView = view;

    if (!myChannel.isConnected()) {
      logger.info("JGroupsMessenger channel is closed - messaging is not possible");
      throw new MembershipClosedException("Distributed System is shutting down");
    }

    filterOutgoingMessage(msg);

    var destinations = msg.getRecipients();
    var allDestinations = msg.forAll();

    var useMcast = false;
    if (services.getConfig().isMulticastEnabled()) {
      if (msg.getMulticast() || allDestinations) {
        useMcast = services.getManager().isMulticastAllowed();
      }
    }

    if (logger.isDebugEnabled() && reliably) {
      var recips = useMcast ? "multicast" : destinations.toString();
      if (logger.isDebugEnabled()) {
        logger.debug("sending via JGroups: [{}] recipients: {}", msg, recips);
      }
    }

    var local = jgAddress;

    Set<ID> failedRecipients = new HashSet<>();
    if (useMcast) {
      var startSer = theStats.startMsgSerialization();
      org.jgroups.Message jmsg;
      try {
        jmsg =
            createJGMessage(msg, local, null, KnownVersion.getCurrentVersion().ordinal());
      } catch (IOException e) {
        return new HashSet<>(msg.getRecipients());
      } finally {
        theStats.endMsgSerialization(startSer);
      }

      Exception problem;
      try {
        jmsg.setTransientFlag(TransientFlag.DONT_LOOPBACK);
        if (!reliably) {
          jmsg.setFlag(org.jgroups.Message.Flag.NO_RELIABILITY);
        }
        theStats.incSentBytes(jmsg.getLength());
        if (logger.isTraceEnabled()) {
          logger.trace("Sending JGroups message: {}", jmsg);
        }
        myChannel.send(jmsg);
      } catch (Exception e) {
        if (logger.isDebugEnabled()) {
          logger.debug("caught unexpected exception", e);
        }
        var cause = e.getCause();
        if (cause instanceof MemberDisconnectedException) {
          problem = (Exception) cause;
        } else {
          problem = e;
        }
        if (services.getShutdownCause() != null) {
          Throwable shutdownCause = services.getShutdownCause();
          // If ForcedDisconnectException occurred then report it as actual
          // problem.
          if (shutdownCause instanceof MemberDisconnectedException) {
            problem = (Exception) shutdownCause;
          } else {
            Throwable ne = problem;
            while (ne.getCause() != null) {
              ne = ne.getCause();
            }
            ne.initCause(services.getShutdownCause());
          }
        }
        final var channelClosed =
            "Channel closed";
        throw new MembershipClosedException(channelClosed, problem);
      }
    } // useMcast
    else { // ! useMcast
      var len = destinations.size();
      List<ID> calculatedMembers; // explicit list of members
      int calculatedLen; // == calculatedMembers.len
      if (len == 1 && destinations.get(0) == ALL_RECIPIENTS) { // send to all
        // Grab a copy of the current membership
        var v = services.getJoinLeave().getView();

        // Construct the list
        calculatedLen = v.size();
        calculatedMembers = new LinkedList<>();
        for (var i = 0; i < calculatedLen; i++) {
          var m = v.get(i);
          calculatedMembers.add(m);
        }
      } // send to all
      else { // send to explicit list
        calculatedLen = len;
        calculatedMembers = new LinkedList<>();
        for (var i = 0; i < calculatedLen; i++) {
          calculatedMembers.add(destinations.get(i));
        }
      } // send to explicit list
      var messages = new Int2ObjectOpenHashMap<org.jgroups.Message>();
      var startSer = theStats.startMsgSerialization();
      var firstMessage = true;
      for (var mbr : calculatedMembers) {
        var version = mbr.getVersionOrdinal();
        if (!messages.containsKey(version)) {
          org.jgroups.Message jmsg;
          try {
            jmsg = createJGMessage(msg, local, mbr, version);
            messages.put(version, jmsg);
          } catch (IOException e) {
            failedRecipients.add(mbr);
            continue;
          }
          if (firstMessage) {
            theStats.incSentBytes(jmsg.getLength());
            firstMessage = false;
          }
        }
      }
      theStats.endMsgSerialization(startSer);
      Collections.shuffle(calculatedMembers);
      var i = 0;
      for (var mbr : calculatedMembers) {
        var to = new JGAddress(mbr);
        var version = mbr.getVersionOrdinal();
        var jmsg = messages.get(version);
        if (jmsg == null) {
          continue; // failed for all recipients
        }
        Exception problem = null;
        try {
          var tmp = (i < (calculatedLen - 1)) ? jmsg.copy(true) : jmsg;
          if (!reliably) {
            jmsg.setFlag(org.jgroups.Message.Flag.NO_RELIABILITY);
          }
          tmp.setDest(to);
          tmp.setSrc(jgAddress);
          if (logger.isTraceEnabled()) {
            logger.trace("Unicasting to {}", to);
          }
          myChannel.send(tmp);
        } catch (Exception e) {
          problem = e;
        }
        if (problem != null) {
          Throwable cause = services.getShutdownCause();
          if (cause != null) {
            // If ForcedDisconnectException occurred then report it as actual
            // problem.
            if (cause instanceof MemberDisconnectedException) {
              problem = (Exception) cause;
            } else {
              Throwable ne = problem;
              while (ne.getCause() != null) {
                ne = ne.getCause();
              }
              ne.initCause(cause);
            }
          }
          final var channelClosed =
              "Channel closed";
          throw new MembershipClosedException(channelClosed, problem);
        }
      } // send individually
    } // !useMcast

    // The contract is that every destination enumerated in the
    // message should have received the message. If one left
    // (i.e., left the view), we signal it here.
    if (failedRecipients.isEmpty() && msg.forAll()) {
      return Collections.emptySet();
    }
    var newView = view;
    if (newView != null && newView != oldView) {
      for (var d : destinations) {
        if (!newView.contains(d)) {
          if (logger.isDebugEnabled()) {
            logger.debug("messenger: member has left the view: {}  view is now {}", d, newView);
          }
          failedRecipients.add(d);
        }
      }
    }
    return failedRecipients;
  }

  /**
   * This is the constructor to use to create a JGroups message holding a GemFire
   * DistributionMessage. It sets the appropriate flags in the Message and properly serializes the
   * DistributionMessage for the recipient's product version
   *
   * @param gfmsg the DistributionMessage
   * @param src the sender address
   * @param versionOrdinal the version of the recipient
   * @return the new message
   */
  org.jgroups.Message createJGMessage(Message<ID> gfmsg, JGAddress src, ID dst,
      short versionOrdinal) throws IOException {
    gfmsg.registerProcessor();
    var msg = new org.jgroups.Message();
    msg.setDest(null);
    msg.setSrc(src);
    setMessageFlags(gfmsg, msg);
    try {
      var start = services.getStatistics().startMsgSerialization();
      final var version = Versioning
          .getKnownVersionOrDefault(Versioning.getVersion(versionOrdinal), KnownVersion.CURRENT);
      try (var out_stream = new BufferDataOutputStream(version)) {
        VersioningIO.writeOrdinal(out_stream, KnownVersion.getCurrentVersion().ordinal(), true);
        if (encrypt != null) {
          out_stream.writeBoolean(true);
          writeEncryptedMessage(gfmsg, dst, versionOrdinal, out_stream);
        } else {
          out_stream.writeBoolean(false);
          serializeMessage(gfmsg, out_stream);
        }

        msg.setBuffer(out_stream.toByteArray());
      }
      services.getStatistics().endMsgSerialization(start);
    } catch (IOException ex) {
      logger.warn("Error serializing message", ex);
      throw ex;
    } catch (Exception ex) {
      logger.warn("Error serializing message", ex);
      throw new IOException("Error serializing message", ex.getCause());
    }
    return msg;
  }

  void writeEncryptedMessage(Message<ID> gfmsg, ID recipient, short versionOrdinal,
      BufferDataOutputStream out)
      throws Exception {
    var start = services.getStatistics().startUDPMsgEncryption();
    try {
      services.getSerializer().writeDSFIDHeader(gfmsg.getDSFID(), out);
      byte[] pk = null;
      var requestId = 0;
      ID pkMbr = null;
      switch (gfmsg.getDSFID()) {
        case FIND_COORDINATOR_REQ:
        case JOIN_REQUEST:
          // need to append mine PK
          pk = encrypt.getPublicKey(localAddress);
          pkMbr = recipient;
          requestId = getRequestId(gfmsg, pkMbr, true);
          break;
        case FIND_COORDINATOR_RESP:
        case JOIN_RESPONSE:
          pkMbr = recipient;
          requestId = getRequestId(gfmsg, pkMbr, false);
        default:
          break;
      }
      if (logger.isDebugEnabled()) {
        logger.debug("writeEncryptedMessage gfmsg.getDSFID() = {} for {} with requestid {}",
            gfmsg.getDSFID(), pkMbr, requestId);
      }
      out.writeInt(requestId);
      if (pk != null) {
        StaticSerialization.writeByteArray(pk, out);
      }

      final var version = Versioning
          .getKnownVersionOrDefault(Versioning.getVersion(versionOrdinal), KnownVersion.CURRENT);
      byte[] messageBytes;
      try (var out_stream = new BufferDataOutputStream(version)) {
        messageBytes = serializeMessage(gfmsg, out_stream);
      }

      if (pkMbr != null) {
        // using members private key
        messageBytes = encrypt.encryptData(messageBytes, pkMbr);
      } else {
        // using cluster secret key
        messageBytes = encrypt.encryptData(messageBytes);
      }
      StaticSerialization.writeByteArray(messageBytes, out);
    } finally {
      services.getStatistics().endUDPMsgEncryption(start);
    }
  }

  int getRequestId(Message<ID> gfmsg, ID destination, boolean add) {
    var requestId = 0;
    if (gfmsg instanceof FindCoordinatorRequest) {
      requestId = ((FindCoordinatorRequest<ID>) gfmsg).getRequestId();
    } else if (gfmsg instanceof JoinRequestMessage) {
      requestId = ((JoinRequestMessage<ID>) gfmsg).getRequestId();
    } else if (gfmsg instanceof FindCoordinatorResponse) {
      requestId = ((FindCoordinatorResponse<ID>) gfmsg).getRequestId();
    } else if (gfmsg instanceof JoinResponseMessage) {
      requestId = ((JoinResponseMessage<ID>) gfmsg).getRequestId();
    }

    if (add) {
      addRequestId(requestId, destination);
    }

    return requestId;
  }

  byte[] serializeMessage(Message<ID> gfmsg, BufferDataOutputStream out_stream)
      throws IOException {
    var m = localAddress;
    m.getMemberData().writeEssentialData(out_stream,
        services.getSerializer().createSerializationContext(out_stream));
    services.getSerializer().getObjectSerializer()
        .writeObject(gfmsg, out_stream);

    return out_stream.toByteArray();
  }

  void setMessageFlags(Message<ID> gfmsg, org.jgroups.Message msg) {
    // Bundling is mostly only useful if we're doing no-ack work,
    // which is fairly rare
    msg.setFlag(Flag.DONT_BUNDLE);

    if (gfmsg.isHighPriority()) {
      msg.setFlag(Flag.OOB);
      msg.setFlag(Flag.NO_FC);
      msg.setFlag(Flag.SKIP_BARRIER);
    }

    msg.setTransientFlag(org.jgroups.Message.TransientFlag.DONT_LOOPBACK);
  }


  /**
   * deserialize a jgroups payload. If it's a DistributionMessage find the ID of the sender and
   * establish it as the message's sender
   */
  Object readJGMessage(org.jgroups.Message jgmsg) {
    Object result = null;

    var messageLength = jgmsg.getLength();

    if (logger.isTraceEnabled()) {
      logger.trace("deserializing a message of length " + messageLength);
    }

    if (messageLength == 0) {
      // jgroups messages with no payload are used for protocol interchange, such
      // as STABLE_GOSSIP
      if (logger.isTraceEnabled()) {
        logger.trace("message length is zero - ignoring");
      }
      return null;
    }

    Exception problem = null;
    var buf = jgmsg.getRawBuffer();
    try {
      var start = services.getStatistics().startMsgDeserialization();

      var dis =
          new DataInputStream(new ByteArrayInputStream(buf, jgmsg.getOffset(), jgmsg.getLength()));

      var ordinal = VersioningIO.readOrdinal(dis);

      if (ordinal < KnownVersion.getCurrentVersion().ordinal()) {
        final var version = Versioning.getKnownVersionOrDefault(
            Versioning.getVersion(ordinal),
            KnownVersion.CURRENT);
        dis = new VersionedDataInputStream(dis,
            version);
      }

      // read
      var isEncrypted = dis.readBoolean();

      if (isEncrypted && encrypt == null) {
        throw new MembershipConfigurationException("Got remote message as encrypted");
      }

      if (isEncrypted) {
        result = readEncryptedMessage(dis, ordinal, encrypt);
      } else {
        result = deserializeMessage(dis, ordinal);
      }


      services.getStatistics().endMsgDeserialization(start);
    } catch (ClassNotFoundException | IOException | RuntimeException e) {
      problem = e;
    } catch (Exception e) {
      problem = e;
    }
    if (problem != null) {
      logger.error(String.format("Exception deserializing message payload: %s", jgmsg),
          problem);
      return null;
    }

    return result;
  }

  void setSender(Message<ID> dm, ID m, short ordinal) {
    ID sender = null;
    // JoinRequestMessages are sent with an ID that may have been
    // reused from a previous life by way of auto-reconnect,
    // so we don't want to find a canonical reference for the
    // request's sender ID
    if (dm.getDSFID() == JOIN_REQUEST) {
      sender = ((JoinRequestMessage<ID>) dm).getMemberID();
    } else {
      sender = getMemberFromView(m, ordinal);
    }
    dm.setSender(sender);
  }

  Message<ID> readEncryptedMessage(DataInputStream dis, short ordinal,
      GMSEncrypt<ID> encryptLocal) throws Exception {
    var dfsid = services.getSerializer().readDSFIDHeader(dis);
    var requestId = dis.readInt();
    var start = services.getStatistics().startUDPMsgDecryption();
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("readEncryptedMessage Reading Request id " + dfsid + " and requestid is "
            + requestId + " myid " + localAddress);
      }
      ID pkMbr = null;
      var readPK = false;
      switch (dfsid) {
        case FIND_COORDINATOR_REQ:
        case JOIN_REQUEST:
          readPK = true;
          break;
        case FIND_COORDINATOR_RESP:
        case JOIN_RESPONSE:
          // this will have requestId to know the PK
          pkMbr = getRequestedMember(requestId);
          break;
      }

      byte[] data;

      byte[] pk = null;

      if (readPK) {
        pk = StaticSerialization.readByteArray(dis);
        data = StaticSerialization.readByteArray(dis);
        // using prefixed pk from sender
        data = encryptLocal.decryptData(data, pk);
      } else {
        data = StaticSerialization.readByteArray(dis);
        // from cluster key
        if (pkMbr != null) {
          // using member public key
          data = encryptLocal.decryptData(data, pkMbr);
        } else {
          // from cluster key
          data = encryptLocal.decryptData(data);
        }
      }

      {
        var in = new DataInputStream(new ByteArrayInputStream(data));

        if (ordinal < KnownVersion.getCurrentVersion().ordinal()) {
          final var version = Versioning.getKnownVersionOrDefault(
              Versioning.getVersion(ordinal),
              KnownVersion.CURRENT);
          in = new VersionedDataInputStream(in,
              version);
        }

        var result = deserializeMessage(in, ordinal);

        if (pk != null) {
          logger.info("Setting public key for " + result.getSender() + " len " + pk.length);
          setPublicKey(pk, result.getSender());
        }

        return result;
      }
    } catch (Exception e) {
      throw new Exception("Message id is " + dfsid, e);
    } finally {
      services.getStatistics().endUDPMsgDecryption(start);
    }

  }

  Message<ID> deserializeMessage(DataInputStream in, short ordinal)
      throws ClassNotFoundException, IOException {
    var info = new GMSMemberData();
    info.readEssentialData(in, services.getSerializer().createDeserializationContext(in));
    var m = services.getMemberFactory().create(info);
    Message<ID> result = services.getSerializer().getObjectDeserializer().readObject(in);

    setSender(result, m, ordinal);

    return result;
  }

  /** look for certain messages that may need to be altered before being sent */
  void filterOutgoingMessage(Message<ID> m) {
    switch (m.getDSFID()) {
      case JOIN_RESPONSE:
        var jrsp = (JoinResponseMessage<ID>) m;

        if (jrsp.getRejectionMessage() == null
            && services.getConfig().isMulticastEnabled()) {
          // get the multicast message digest and pass it with the join response
          var digest = (Digest) myChannel.getProtocolStack().getTopProtocol()
              .down(Event.GET_DIGEST_EVT);
          try (var bufferDataOutputStream =
              new BufferDataOutputStream(500, KnownVersion.CURRENT)) {
            try {
              digest.writeTo(bufferDataOutputStream);
            } catch (Exception e) {
              logger.fatal("Unable to serialize JGroups messaging digest", e);
            }
            jrsp.setMessengerData(bufferDataOutputStream.toByteArray());
          }
        }
        break;
      default:
        break;
    }
  }

  void filterIncomingMessage(Message<ID> m) {
    switch (m.getDSFID()) {
      case JOIN_RESPONSE:
        var jrsp = (JoinResponseMessage<ID>) m;

        if (jrsp.getRejectionMessage() == null
            && services.getConfig().isMulticastEnabled()) {
          var serializedDigest = jrsp.getMessengerData();
          var bis = new ByteArrayInputStream(serializedDigest);
          var dis = new DataInputStream(bis);
          try {
            var digest = new Digest();
            digest.readFrom(dis);
            if (logger.isTraceEnabled()) {
              logger.trace("installing JGroups message digest {} from {}", digest, m);
            }
            myChannel.getProtocolStack().getTopProtocol()
                .down(new Event(Event.MERGE_DIGEST, digest));
            jrsp.setMessengerData(null);
          } catch (Exception e) {
            logger.fatal("Unable to read JGroups messaging digest", e);
          }
        }
        break;
      default:
        break;
    }
  }

  @Override
  public ID getMemberID() {
    return localAddress;
  }

  /**
   * returns the member ID for the given GMSMember object
   */
  @SuppressWarnings("UnusedParameters")
  private ID getMemberFromView(ID jgId, short version) {
    return services.getJoinLeave().getMemberID(jgId);
  }


  @Override
  public void emergencyClose() {
    view = null;
    if (myChannel != null) {
      if ((services.isShutdownDueToForcedDisconnect() && services.isAutoReconnectEnabled())
          || services.getManager().isReconnectingDS()) {
      } else {
        myChannel.disconnect();
      }
    }
  }

  @Override
  public GMSQuorumChecker<ID> getQuorumChecker() {
    var view = this.view;
    if (view == null) {
      view = services.getJoinLeave().getView();
      if (view == null) {
        view = services.getJoinLeave().getPreviousView();
        if (view == null) {
          return null;
        }
      }
    }
    var qc =
        new GMSQuorumChecker<ID>(view, services.getConfig().getLossThreshold(), myChannel,
            encrypt);
    qc.initialize();
    return qc;
  }

  /**
   * JGroupsReceiver receives incoming JGroups messages and passes them to a handler. It may be
   * accessed through JChannel.getReceiver().
   */
  class JGroupsReceiver extends ReceiverAdapter {

    @Override
    public void receive(org.jgroups.Message jgmsg) {
      receive(jgmsg, false);
    }

    protected void receive(org.jgroups.Message jgmsg, boolean fromQuorumChecker) {
      var startTime = services.getStatistics().startUDPDispatchRequest();
      try {
        if (services.getManager().shutdownInProgress()) {
          return;
        }

        if (logger.isTraceEnabled()) {
          logger.trace("JGroupsMessenger received {} headers: {}", jgmsg, jgmsg.getHeaders());
        }

        // Respond to ping messages sent from other systems that are in a auto reconnect state
        var contents = jgmsg.getBuffer();
        if (contents == null) {
          return;
        }
        if (pingPonger.isPingMessage(contents)) {
          try {
            pingPonger.sendPongMessage(myChannel, jgAddress, jgmsg.getSrc());
          } catch (Exception e) {
            logger.info("Failed sending Pong response to " + jgmsg.getSrc());
          }
          return;
        } else if (pingPonger.isPongMessage(contents)) {
          pongsReceived.incrementAndGet();
          return;
        }

        var o = readJGMessage(jgmsg);
        if (o == null) {
          return;
        }

        var msg = (Message<ID>) o;

        // admin-only VMs don't have caches, so we ignore cache operations
        // multicast to them, avoiding deserialization cost and classpath
        // problems
        if ((services.getConfig()
            .getVmKind() == MemberIdentifier.ADMIN_ONLY_DM_TYPE)
            && (msg instanceof CacheOperationMessageMarker)) {
          return;
        }

        msg.resetTimestamp();
        msg.setBytesRead(jgmsg.getLength());

        try {

          if (logger.isTraceEnabled()) {
            logger.trace("JGroupsMessenger dispatching {} from {}", msg, msg.getSender());
          }
          filterIncomingMessage(msg);
          var handler = getMessageHandler(msg);
          if (fromQuorumChecker
              && (handler instanceof HealthMonitor || handler instanceof Manager)) {
            // ignore suspect / heartbeat messages that happened during
            // auto-reconnect because they very likely have old member IDs in them.
            // Also ignore non-membership messages because we weren't a member when we received
            // them.
          } else {
            handler.processMessage(msg);
          }

          // record the scheduling of broadcast messages
          var header = (NakAckHeader2) jgmsg.getHeader(nackack2HeaderId);
          if (header != null && !jgmsg.isFlagSet(Flag.OOB)) {
            recordScheduledSeqno(msg.getSender(), header.getSeqno());
          }

        } catch (MemberShunnedException e) {
          // message from non-member - ignore
        }

      } finally {
        services.getStatistics().endUDPDispatchRequest(startTime);
      }
    }

    private void recordScheduledSeqno(ID member, long seqno) {
      synchronized (scheduledMcastSeqnos) {
        var counter = scheduledMcastSeqnos.get(member);
        if (counter == null) {
          counter = new MessageTracker(seqno);
          scheduledMcastSeqnos.put(member, counter);
        }
        counter.record(seqno);
      }
    }

    /**
     * returns the handler that should process the given message. The default handler is the
     * membership manager
     */
    private MessageHandler<Message<ID>> getMessageHandler(Message<ID> msg) {
      Class<?> msgClazz = msg.getClass();
      var h = handlers.get(msgClazz);
      if (h == null) {
        for (var clazz : handlers.keySet()) {
          if (clazz.isAssignableFrom(msgClazz)) {
            h = handlers.get(clazz);
            handlers.put(msg.getClass(), h);
            break;
          }
        }
      }
      if (h == null) {
        h = services.getManager();
      }
      return (MessageHandler<Message<ID>>) h;
    }
  }

  @Override
  public Set<ID> send(Message<ID> msg, GMSMembershipView<ID> alternateView) {
    if (encrypt != null) {
      encrypt.installView(alternateView);
    }
    return send(msg, true);
  }

  @Override
  public byte[] getPublicKey(ID mbr) {
    if (encrypt != null) {
      return encrypt.getPublicKey(mbr);
    }
    return null;
  }

  @Override
  public void setPublicKey(byte[] publickey, ID mbr) {
    if (encrypt != null) {
      if (logger.isDebugEnabled()) {
        logger.debug("Setting PK for member " + mbr);
      }
      encrypt.setPublicKey(publickey, mbr);
    }
  }

  @Override
  public void setClusterSecretKey(byte[] clusterSecretKey) {
    if (encrypt != null) {
      if (logger.isDebugEnabled()) {
        logger.debug("Setting cluster key");
      }
      encrypt.setClusterKey(clusterSecretKey);
    }
  }

  @Override
  public byte[] getClusterSecretKey() {
    if (encrypt != null) {
      return encrypt.getClusterSecretKey();
    }
    return null;
  }

  private final AtomicInteger requestId = new AtomicInteger((new Random().nextInt()));
  private final HashMap<Integer, ID> requestIdVsRecipients = new HashMap<>();

  ID getRequestedMember(int requestId) {
    return requestIdVsRecipients.remove(requestId);
  }

  void addRequestId(int requestId, ID mbr) {
    requestIdVsRecipients.put(requestId, mbr);
  }

  @Override
  public int getRequestId() {
    return requestId.incrementAndGet();
  }

  @Override
  public void initClusterKey() {
    if (encrypt != null) {
      try {
        logger.info("Initializing cluster key");
        encrypt.initClusterSecretKey();
      } catch (Exception e) {
        throw new RuntimeException("unable to create cluster key ", e);
      }
    }
  }

  static class MessageTracker {
    long highestSeqno;

    MessageTracker(long seqno) {
      highestSeqno = seqno;
    }

    long get() {
      return highestSeqno;
    }

    void record(long seqno) {
      if (seqno > highestSeqno) {
        highestSeqno = seqno;
      }
    }
  }
}
