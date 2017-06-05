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

package org.talend.components.netsuite.input;

import java.util.EnumSet;
import java.util.Set;

import org.talend.components.api.component.ConnectorTopology;
import org.talend.components.api.component.runtime.ExecutionEngine;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.netsuite.NetSuiteComponentDefinition;
import org.talend.daikon.runtime.RuntimeInfo;

/**
 * Definition of NetSuite Input component.
 *
 * <p>Input component is responsible for searching of NetSuite records.
 */
public class NetSuiteInputDefinition extends NetSuiteComponentDefinition {

    public static final String COMPONENT_NAME = "tNetsuiteInput"; //$NON-NLS-1$

    public NetSuiteInputDefinition() {
        super(COMPONENT_NAME, ExecutionEngine.DI);
    }

    @Override
    public boolean isStartable() {
        return true;
    }

    @Override
    public RuntimeInfo getRuntimeInfo(ExecutionEngine engine, ComponentProperties properties,
            ConnectorTopology connectorTopology) {
        assertEngineCompatibility(engine);
        if (connectorTopology != ConnectorTopology.NONE) {
            assertConnectorTopologyCompatibility(connectorTopology);
        }
        NetSuiteInputProperties inputProperties = (NetSuiteInputProperties) properties;
        return getRuntimeInfo(inputProperties, NetSuiteComponentDefinition.SOURCE_CLASS);
    }

    @Override
    public Set<ConnectorTopology> getSupportedConnectorTopologies() {
        return EnumSet.of(ConnectorTopology.OUTGOING);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<? extends ComponentProperties>[] getNestedCompatibleComponentPropertiesClass() {
        return concatPropertiesClasses(super.getNestedCompatibleComponentPropertiesClass(),
                new Class[] { NetSuiteInputModuleProperties.class });
    }

    @Override
    public Class<? extends ComponentProperties> getPropertyClass() {
        return NetSuiteInputProperties.class;
    }

}
