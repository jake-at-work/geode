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
package org.apache.geode.sequence;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Toolkit;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.ScrollPaneConstants;

/**
 * Created by IntelliJ IDEA. User: dan Date: Oct 28, 2010 Time: 10:29:23 PM To change this template
 * use File | Settings | File Templates.
 */
public class SequencePanel extends JPanel {

  public SequencePanel(SequenceDiagram sequenceDiagram) {
    // Set up the drawing area.
    var drawingPane = new ZoomingPanel();
    drawingPane.setBackground(Color.white);
    drawingPane.setSequenceDiagram(sequenceDiagram);

    // Put the drawing area in a scroll pane.
    final var scroller = new JScrollPane(drawingPane);
    scroller.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);
    scroller.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_ALWAYS);
    final var timeAxis =
        new TimeAxis(TimeAxis.VERTICAL, sequenceDiagram.getMinTime(), sequenceDiagram.getMaxTime());
    timeAxis.setPreferredHeight(drawingPane.getHeight());
    scroller.setRowHeaderView(timeAxis);
    scroller.setColumnHeaderView(sequenceDiagram.createMemberAxis());
    var preferredWidth = (int) (Toolkit.getDefaultToolkit().getScreenSize().getWidth() - 100);
    var preferredHeight = (int) (Toolkit.getDefaultToolkit().getScreenSize().getHeight() - 100);
    scroller.setPreferredSize(new Dimension(preferredWidth, preferredHeight));
    scroller.setAutoscrolls(true);
    // scroller.setPreferredSize(new Dimension(200,200));

    sequenceDiagram.addComponentListener(new ComponentAdapter() {
      @Override
      public void componentResized(ComponentEvent e) {
        var height = e.getComponent().getHeight();
        timeAxis.setPreferredHeight(height);
        timeAxis.revalidate();
      }
    });

    var layout = new BorderLayout();
    // layout.setHgap(0);
    // layout.setVgap(0);
    setLayout(layout);
    // Lay out this demo.
    // add(instructionPanel, BorderLayout.PAGE_START);
    add(scroller, BorderLayout.CENTER);

    addComponentListener(new ComponentAdapter() {
      @Override
      public void componentResized(ComponentEvent e) {
        var source = e.getComponent();
        scroller.setSize(source.getSize());
        scroller.revalidate();
      }
    });


  }
}
