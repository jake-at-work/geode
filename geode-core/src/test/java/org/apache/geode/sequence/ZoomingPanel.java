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

import java.awt.AlphaComposite;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseMotionAdapter;

import javax.swing.JPanel;
import javax.swing.JViewport;

/**
 * Created by IntelliJ IDEA. User: dan Date: Oct 28, 2010 Time: 10:30:40 PM To change this template
 * use File | Settings | File Templates.
 */
public class ZoomingPanel extends JPanel {
  private int zoomBoxStartX;
  private int zoomBoxStartY;
  private int zoomBoxWidth;
  private int zoomBoxHeight;
  private SequenceDiagram child;


  public ZoomingPanel() {
    super();
    addMouseListener(new MouseAdapter() {
      @Override
      public void mousePressed(MouseEvent e) {
        startBox(e.getX(), e.getY());
      }

      @Override
      public void mouseReleased(MouseEvent e) {
        endBox(e.getX(), e.getY());
      }

      @Override
      public void mouseClicked(MouseEvent e) {
        if (e.getButton() != MouseEvent.BUTTON1) {
          unzoom();
        } else {
          child.selectState(e.getX(), e.getY());
        }

      }
    });

    addMouseMotionListener(new MouseMotionAdapter() {
      @Override
      public void mouseDragged(MouseEvent e) {
        var r = new Rectangle(e.getX(), e.getY(), 1, 1);
        ((JPanel) e.getSource()).scrollRectToVisible(r);
        showBox(e.getX(), e.getY());
      }

      @Override
      public void mouseMoved(MouseEvent e) {
        var popupX = getLocationOnScreen().x + e.getX();
        var popupY = getLocationOnScreen().y + e.getY();
        child.showPopupText(e.getX(), e.getY(), popupX, popupY);
      }
    });
    var layout = new BorderLayout();
    layout.setHgap(0);
    layout.setVgap(0);
    setLayout(layout);
  }

  private void unzoom() {
    resizeMe(0, 0, getWidth(), getHeight());
  }

  void resizeMe(int zoomBoxX, int zoomBoxY, int zoomBoxWidth, int zoomBoxHeight) {
    var viewSize = getParent().getSize();
    var windowWidth = viewSize.getWidth();
    var windowHeight = viewSize.getHeight();
    var scaleX = getWidth() / ((double) zoomBoxWidth);
    var scaleY = getHeight() / ((double) zoomBoxHeight);
    var oldWidth = getWidth();
    var oldHeight = getHeight();
    var width = (int) (scaleX * windowWidth);
    var height = (int) (scaleY * windowHeight);
    // this.setPreferredSize(new Dimension(width, height));
    child.resizeMe(width, height);
    // TODO not sure this one is needed
    revalidate();

    // scroll to the new rectangle
    // int scrollX = (int) (zoomBoxX * scaleX);
    // int scrollY = (int) (zoomBoxY * scaleY);
    // int scrollWidth= (int) (zoomBoxWidth * scaleX);
    // int scrollHeight = (int) (zoomBoxHeight * scaleY);
    var scrollX = (int) (zoomBoxX * (width / (double) oldWidth));
    var scrollY = (int) (zoomBoxY * (height / (double) oldHeight));
    var scrollWidth = (int) (zoomBoxWidth * (width / (double) oldWidth));
    var scrollHeight = (int) (zoomBoxHeight * (height / (double) oldHeight));
    var r = new Rectangle(scrollX, scrollY, scrollWidth, scrollHeight);
    ((JViewport) getParent()).scrollRectToVisible(r);
    repaint();

  }

  public void setSequenceDiagram(SequenceDiagram diag) {
    child = diag;
    add(child, BorderLayout.CENTER);
  }

  private void showBox(int x, int y) {
    if (zoomBoxWidth != -1) {
      repaint(getBoxX(), getBoxY(), getBoxWidth(), getBoxHeight());
    }

    zoomBoxWidth = x - zoomBoxStartX;
    zoomBoxHeight = y - zoomBoxStartY;

    repaint(getBoxX(), getBoxY(), getBoxWidth(), getBoxHeight());
  }

  private void startBox(int x, int y) {
    zoomBoxStartX = x;
    zoomBoxStartY = y;
  }

  private void endBox(int x, int y) {
    if (zoomBoxStartX != -1 && zoomBoxStartY != -1 && zoomBoxWidth != -1 && zoomBoxHeight != -1
        && zoomBoxWidth != 0 && zoomBoxHeight != 0) {
      resizeMe(getBoxX(), getBoxY(), getBoxWidth(), getBoxHeight());
      repaint(getBoxX(), getBoxY(), getBoxWidth(), getBoxHeight());
      zoomBoxStartX = -1;
      zoomBoxStartY = -1;
      zoomBoxWidth = -1;
      zoomBoxHeight = -1;
    }
  }

  public int getBoxX() {
    return zoomBoxWidth >= 0 ? zoomBoxStartX : zoomBoxStartX + zoomBoxWidth;
  }

  public int getBoxY() {
    return zoomBoxHeight >= 0 ? zoomBoxStartY : zoomBoxStartY + zoomBoxHeight;
  }

  public int getBoxHeight() {
    return zoomBoxHeight >= 0 ? zoomBoxHeight : -zoomBoxHeight;
  }

  public int getBoxWidth() {
    return zoomBoxWidth >= 0 ? zoomBoxWidth : -zoomBoxWidth;
  }



  @Override
  public void paint(Graphics g) {
    super.paint(g);

    if (zoomBoxStartX != -1 && zoomBoxStartY != -1 && zoomBoxWidth != -1 && zoomBoxHeight != -1) {
      var g2 = (Graphics2D) g.create();

      var old = g2.getComposite();
      g2.setComposite(AlphaComposite.getInstance(AlphaComposite.SRC_OVER, 0.5f));
      g2.setColor(Color.BLUE);

      // g2.drawRect(zoomBoxStartX, zoomBoxStartY, zoomBoxWidth, zoomBoxHeight);
      // g2.setBackground(Color.BLUE);
      g2.fillRect(getBoxX(), getBoxY(), getBoxWidth(), getBoxHeight());
      g2.setComposite(old);
    }
  }
}
