// ============================================================================
//
// Copyright (C) 2006-2017 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.marketo.tmarketoinput;

import java.util.EnumSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.api.component.ConnectorTopology;
import org.talend.components.api.component.runtime.ExecutionEngine;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.marketo.MarketoComponentDefinition;
import org.talend.daikon.runtime.RuntimeInfo;

public class TMarketoInputDefinition extends MarketoComponentDefinition {

    public static final String COMPONENT_NAME = "tMarketoInputDEV";

    private transient static final Logger LOG = LoggerFactory.getLogger(TMarketoInputDefinition.class);

    public TMarketoInputDefinition() {
        super(COMPONENT_NAME);
    }

    @Override
    public Class<? extends ComponentProperties> getPropertyClass() {
        return TMarketoInputProperties.class;
    }

    @Override
    public Set<ConnectorTopology> getSupportedConnectorTopologies() {
        return EnumSet.of(ConnectorTopology.INCOMING_AND_OUTGOING, ConnectorTopology.OUTGOING);
    }

    @Override
    public boolean isConditionalInputs() {
        return true;
    }

    @Override
    public RuntimeInfo getRuntimeInfo(ExecutionEngine engine, ComponentProperties properties,
            ConnectorTopology connectorTopology) {
        LOG.warn("getRuntimeInfo {} properties {} topology {}", engine, connectorTopology);
        assertEngineCompatibility(engine);
        // assertConnectorTopologyCompatibility(connectorTopology);
        if (connectorTopology == ConnectorTopology.OUTGOING) {
            return getCommonRuntimeInfo(this.getClass().getClassLoader(), RUNTIME_SOURCE_CLASS);
        } else {
            return getCommonRuntimeInfo(this.getClass().getClassLoader(), RUNTIME_SINK_CLASS);
        }
    }
}