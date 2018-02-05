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
package org.talend.components.google.drive.runtime;

import static org.junit.Assert.*;
import static org.slf4j.LoggerFactory.getLogger;
import static org.talend.components.google.drive.runtime.GoogleDriveRuntime.getStudioName;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.slf4j.Logger;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.google.drive.connection.GoogleDriveConnectionProperties;
import org.talend.components.google.drive.connection.GoogleDriveConnectionProperties.OAuthMethod;
import org.talend.components.google.drive.create.GoogleDriveCreateDefinition;
import org.talend.components.google.drive.create.GoogleDriveCreateProperties;

public class GoogleDriveBaseTestIT {

    public static final String GOOGLEDRIVE_SERVICE_ACCOUNT_PROP = "org.talend.components.google.drive.service_account_file";

    public static String GOOGLEDRIVE_SERVICE_ACCOUNT_FILE;

    protected GoogleDriveSource source;

    protected GoogleDriveSink sink;

    protected GoogleDriveConnectionProperties connectionProperties;

    protected GoogleDriveCreateProperties createProperties;

    protected GoogleDriveCreateRuntime createRuntime;

    protected RuntimeContainer container = new RuntimeContainer() {

        private Map<String, Object> map = new HashMap<>();

        @Override
        public Object getComponentData(String componentId, String key) {
            return map.get(componentId + "_" + key);
        }

        @Override
        public void setComponentData(String componentId, String key, Object data) {
            map.put(componentId + "_" + key, data);
        }

        @Override
        public String getCurrentComponentId() {
            return "GoogleDrive-ITs";
        }

        @Override
        public Object getGlobalData(String key) {
            return null;
        }

    };

    protected transient static final Logger LOG = getLogger(GoogleDriveBaseTestIT.class);

    static {
        GOOGLEDRIVE_SERVICE_ACCOUNT_FILE = System.getProperty(GOOGLEDRIVE_SERVICE_ACCOUNT_PROP);
    }

    @Before
    public void setUp() throws Exception {
        source = new GoogleDriveSource();
        sink = new GoogleDriveSink();
        connectionProperties = new GoogleDriveConnectionProperties("test");
        connectionProperties.setupProperties();
        connectionProperties.setupLayout();
        connectionProperties.applicationName.setValue("TCOMP-IT");
        connectionProperties.oAuthMethod.setValue(OAuthMethod.ServiceAccount);
        connectionProperties.serviceAccountFile.setValue(GOOGLEDRIVE_SERVICE_ACCOUNT_FILE);
        connectionProperties.afterOAuthMethod();
        //
        createProperties = new GoogleDriveCreateProperties("test");
        createProperties.init();
        createProperties.setupProperties();
        createProperties.connection = connectionProperties;
        createProperties.parentFolder.setValue("root");
        createRuntime = new GoogleDriveCreateRuntime();
    }

    protected void createFolderAtRoot(String folder) throws GeneralSecurityException, IOException {
        createProperties.newFolder.setValue(folder);
        createRuntime.initialize(container, createProperties);
        createRuntime.runAtDriver(container);
        assertNotNull(container.getComponentData(container.getCurrentComponentId(),
                getStudioName(GoogleDriveCreateDefinition.RETURN_NEW_FOLDER_ID)));
    }
}
