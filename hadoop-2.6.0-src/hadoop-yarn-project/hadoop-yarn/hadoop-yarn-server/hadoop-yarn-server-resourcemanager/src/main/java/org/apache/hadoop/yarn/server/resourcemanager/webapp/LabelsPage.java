/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebApp.NODE_STATE;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

class LabelsPage extends RmView {

  static class LabelsBlock extends HtmlBlock {
    final RMContext rmContext;
    final ResourceManager rm;

    @Inject
    LabelsBlock(RMContext context, ResourceManager rm, ViewContext ctx) {
      super(ctx);
      this.rmContext = context;
      this.rm = rm;
    }

    @Override
    protected void render(Block html) {
      html._(MetricsOverviewTable.class);

      ResourceScheduler sched = rm.getResourceScheduler();
      String type = $(NODE_STATE);
      TBODY<TABLE<Hamlet>> tbody = html.table("#labels").
          thead().
          tr().
          th(".nodeids", "Node Ids").
          th(".devices", "Devices").
          th(".accelerators", "Accelerators").
          _()._().
          tbody();
      NodeState stateFilter = null;
      if(type != null && !type.isEmpty()) {
        stateFilter = NodeState.valueOf(type.toUpperCase());
      }
      Collection<RMNode> rmNodes = this.rmContext.getRMNodes().values();
      boolean isInactive = false;
      if (stateFilter != null) {
        switch (stateFilter) {
        case DECOMMISSIONED:
        case LOST:
        case REBOOTED:
          rmNodes = this.rmContext.getInactiveRMNodes().values();
          isInactive = true;
          break;
        }
      }
      for (RMNode ni : rmNodes) {
        if(stateFilter != null) {
          NodeState state = ni.getState();
          if(!stateFilter.equals(state)) {
            continue;
          }
        } else {
          // No filter. User is asking for all nodes. Make sure you skip the
          // unhealthy nodes.
          if (ni.getState() == NodeState.UNHEALTHY) {
            continue;
          }
        }
        NodeInfo info = new NodeInfo(ni, sched);
        HashMap<String, HashSet<String>> labelRelations = info.getNodeLabelRelations();
        for (String parentLabel : labelRelations.keySet()) {
          HashSet<String> labels = labelRelations.get(parentLabel);
          for(String label : labels) {
            TR<TBODY<TABLE<Hamlet>>> row = tbody.tr().
              td(info.getNodeId()).
              td(parentLabel).
              td(label);
            row._();
          }
        }
      }
      tbody._()._();
    }
  }

  @Override protected void preHead(Page.HTML<_> html) {
    commonPreHead(html);
    String type = $(NODE_STATE);
    String title = "Labels of the cluster";
    if(type != null && !type.isEmpty()) {
      title = title+" ("+type+")";
    }
    setTitle(title);
    set(DATATABLES_ID, "labels");
    set(initID(DATATABLES, "labels"), labelsTableInit());
    setTableStyles(html, "labels", ".nodeids {width:10em}", ".devices {width:10em}",
        ".accelerators {width:20em}");
  }

  @Override protected Class<? extends SubView> content() {
    return LabelsBlock.class;
  }

  private String labelsTableInit() {
    StringBuilder b = tableInit().append(", aoColumnDefs: []}");
    return b.toString();
  }
}
