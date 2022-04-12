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
package org.apache.geode.internal.statistics;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Text;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXParseException;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.StatisticDescriptor;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.classloader.ClassPathLoader;

// @todo davidw Use a SAX parser instead of DOM
/**
 * This is an internal helper class for dealing with the SessionFactory XML configuration files.
 */
public class StatisticsTypeXml implements EntityResolver, ErrorHandler {

  /** The name of the DTD file */
  static final String DTD = "statisticsType.dtd";

  static final String systemId = "http://www.gemstone.com/dtd/" + DTD;
  static final String publicId = "-//GemStone Systems, Inc.//GemFire StatisticsType//EN";

  ///////////////////// Interface methods ///////////////////////

  /**
   * Given a publicId, attempts to resolve it to a DTD. Returns an <code>InputSource</code> for the
   * DTD.
   */
  @Override
  public InputSource resolveEntity(String publicId, String systemId) throws SAXException {

    // Figure out the location for the publicId. Be tolerant of other
    // versions of the dtd
    if (publicId.equals(StatisticsTypeXml.publicId) || systemId.equals(StatisticsTypeXml.systemId)
        || systemId.endsWith(DTD)) {

      // Public ID for system config DTD
      var location = "/org/apache/geode/" + DTD;
      var stream = ClassPathLoader.getLatest().getResourceAsStream(getClass(), location);
      if (stream != null) {
        return new InputSource(stream);

      } else {
        throw new SAXNotRecognizedException(
            String.format("DTD not found: %s", location));
      }

    } else {
      throw new SAXNotRecognizedException(
          String.format("Invalid public ID: ' %s '", publicId));
    }
  }

  @Override
  public void warning(SAXParseException exception) throws SAXException {
    // We don't want to thrown an exception. We want to log it!!
    // FIXME
    // String s = "SAX warning while working with XML";
  }

  @Override
  public void error(SAXParseException exception) throws SAXException {
    throw new GemFireConfigException(
        "SAX error while working with XML",
        exception);
  }

  @Override
  public void fatalError(SAXParseException exception) throws SAXException {
    throw new GemFireConfigException(
        "SAX fatal error while working with XML",
        exception);
  }

  ////////////////////// Parsing XML File ////////////////////////

  /**
   * Parses the contents of XML data and from it creates one or more <code>StatisticsType</code>
   * instances.
   */
  public StatisticsType[] read(Reader reader, StatisticsTypeFactory statFactory) {
    var factory = DocumentBuilderFactory.newInstance();
    // factory.setValidating(validate);

    DocumentBuilder parser = null;
    try {
      parser = factory.newDocumentBuilder();

    } catch (ParserConfigurationException ex) {
      throw new GemFireConfigException(
          "Failed parsing XML", ex);
    }

    parser.setErrorHandler(this);
    parser.setEntityResolver(this);
    Document doc;
    try {
      doc = parser.parse(new InputSource(reader));
    } catch (SAXException se) {
      throw new GemFireConfigException(
          "Failed parsing XML", se);
    } catch (IOException io) {
      throw new GemFireConfigException(
          "Failed reading XML data", io);
    }

    if (doc == null) {
      throw new GemFireConfigException(
          "Failed reading XML data; no document");
    }
    var root = doc.getDocumentElement();
    if (root == null) {
      throw new GemFireConfigException(
          "Failed reading XML data; no root element");
    }
    return extractStatistics(root, statFactory);
  }

  /*
   * <!ELEMENT statistics (type)+>
   */
  private StatisticsType[] extractStatistics(Element root, StatisticsTypeFactory statFactory) {
    Assert.assertTrue(root.getTagName().equals("statistics"));

    var types = new ArrayList();
    var typeNodes = root.getElementsByTagName("type");
    for (var i = 0; i < typeNodes.getLength(); i++) {
      var typeNode = (Element) typeNodes.item(i);
      types.add(extractType(typeNode, statFactory));
    }
    return (StatisticsType[]) types.toArray(new StatisticsType[0]);
  }

  /**
   * <!ELEMENT type (description?, (stat)+)> <!ATTLIST type name CDATA #REQUIRED>
   */
  private StatisticsType extractType(Element typeNode, StatisticsTypeFactory statFactory) {
    Assert.assertTrue(typeNode.getTagName().equals("type"));
    Assert.assertTrue(typeNode.hasAttribute("name"));

    final var typeName = typeNode.getAttribute("name");
    var stats = new ArrayList();
    var statNodes = typeNode.getElementsByTagName("stat");
    for (var i = 0; i < statNodes.getLength(); i++) {
      var statNode = (Element) statNodes.item(i);
      stats.add(extractStat(statNode, statFactory));
    }
    var descriptors =
        (StatisticDescriptor[]) stats.toArray(new StatisticDescriptor[0]);
    var description = "";
    {
      var descriptionNodes = typeNode.getElementsByTagName("description");
      if (descriptionNodes.getLength() > 0) {
        // descriptionNodes will contain the both our description, if it exists,
        // and any nested stat descriptions. Ours will always be first
        var descriptionNode = (Element) descriptionNodes.item(0);
        // but make sure the first one belongs to our node
        if (descriptionNode.getParentNode().getNodeName().equals(typeNode.getNodeName())) {
          description = extractDescription(descriptionNode);
        }
      }
    }

    return statFactory.createType(typeName, description, descriptors);
  }

  private static final int INT_STORAGE = 0;
  private static final int LONG_STORAGE = 1;
  private static final int DOUBLE_STORAGE = 2;

  /**
   * <!ELEMENT stat (description?, unit?)> <!ATTLIST stat name CDATA #REQUIRED counter (true |
   * false) #IMPLIED largerBetter (true | false) #IMPLIED storage (int | long | double) #IMPLIED >
   */
  private StatisticDescriptor extractStat(Element statNode, StatisticsTypeFactory statFactory) {
    Assert.assertTrue(statNode.getTagName().equals("stat"));
    Assert.assertTrue(statNode.hasAttribute("name"));

    final var statName = statNode.getAttribute("name");
    var description = "";
    var unit = "";
    var isCounter = true;
    boolean largerBetter;
    var storage = INT_STORAGE;

    if (statNode.hasAttribute("counter")) {
      var value = statNode.getAttribute("counter");
      Assert.assertTrue(value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false"));
      isCounter = Boolean.parseBoolean(value);
    }
    largerBetter = isCounter; // default
    if (statNode.hasAttribute("largerBetter")) {
      var value = statNode.getAttribute("largerBetter");
      Assert.assertTrue(value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false"));
      largerBetter = Boolean.parseBoolean(value);
    }
    if (statNode.hasAttribute("storage")) {
      var value = statNode.getAttribute("storage");
      if (value.equalsIgnoreCase("int")) {
        storage = INT_STORAGE;
      } else if (value.equalsIgnoreCase("long")) {
        storage = LONG_STORAGE;
      } else {
        Assert.assertTrue(value.equalsIgnoreCase("double"));
        storage = DOUBLE_STORAGE;
      }
    }
    {
      var descriptionNodes = statNode.getElementsByTagName("description");
      Assert.assertTrue(descriptionNodes.getLength() <= 1);
      if (descriptionNodes.getLength() == 1) {
        var descriptionNode = (Element) descriptionNodes.item(0);
        description = extractDescription(descriptionNode);
      }
    }

    {
      var unitNodes = statNode.getElementsByTagName("unit");
      Assert.assertTrue(unitNodes.getLength() <= 1);
      if (unitNodes.getLength() == 1) {
        var unitNode = (Element) unitNodes.item(0);
        unit = extractUnit(unitNode);
      }
    }
    if (isCounter) {
      switch (storage) {
        case INT_STORAGE:
          return statFactory.createIntCounter(statName, description, unit, largerBetter);
        case LONG_STORAGE:
          return statFactory.createLongCounter(statName, description, unit, largerBetter);
        case DOUBLE_STORAGE:
          return statFactory.createDoubleCounter(statName, description, unit, largerBetter);
        default:
          throw new RuntimeException(String.format("unexpected storage type %s",
              storage));
      }
    } else {
      switch (storage) {
        case INT_STORAGE:
          return statFactory.createIntGauge(statName, description, unit, largerBetter);
        case LONG_STORAGE:
          return statFactory.createLongGauge(statName, description, unit, largerBetter);
        case DOUBLE_STORAGE:
          return statFactory.createDoubleGauge(statName, description, unit, largerBetter);
        default:
          throw new RuntimeException(String.format("unexpected storage type %s",
              storage));
      }
    }
  }

  /**
   * <!ELEMENT description (#PCDATA)>
   */
  private String extractDescription(Element descriptionNode) {
    Assert.assertTrue(descriptionNode.getTagName().equals("description"));
    return extractText(descriptionNode);
  }

  /**
   * <!ELEMENT unit (#PCDATA)>
   */
  private String extractUnit(Element unitNode) {
    Assert.assertTrue(unitNode.getTagName().equals("unit"));
    return extractText(unitNode);
  }

  private String extractText(Element element) {
    var text = (Text) element.getFirstChild();
    return ((text == null ? "" : text.getData()));
  }
}
