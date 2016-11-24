
// ============================================================================
//
// Copyright (C) 2006-2016 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.udp;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;

import org.talend.components.api.component.runtime.BoundedReader;
import org.talend.components.api.component.runtime.BoundedSource;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.api.properties.ComponentProperties;

import org.talend.daikon.NamedThing;
import org.talend.daikon.properties.ValidationResult;

/**
 * The tUdpSocketInputSource provides the mechanism to supply data to other
 * components at run-time.
 *
 * Based on the Apache Beam project, the Source mechanism is appropriate to
 * describe distributed and non-distributed data sources and can be adapted
 * to scalable big data execution engines on a cluster, or run locally.
 *
 * This example component describes an input source that is guaranteed to be
 * run in a single JVM (whether on a cluster or locally), so:
 *
 * <ul>
 * <li>the simplified logic for reading is found in the {@link tUdpSocketInputReader},
 *     and</li>
 * </ul>
 */
public class tUdpSocketInputSource implements BoundedSource {

    /** Default serial version UID. */
    private static final long serialVersionUID = 1L;

    /** Configuration extracted from the input properties. */
    private tUdpSocketInputProperties properties;
    
    private transient Schema schema;

    public ValidationResult initialize(RuntimeContainer container, ComponentProperties properties) {
        this.properties = (tUdpSocketInputProperties) properties;
        schema = new Schema.Parser().parse(this.properties.schema.schema.getStringValue());
        return ValidationResult.OK;
    }

    public BoundedReader createReader(RuntimeContainer container) {
        return new tUdpSocketInputReader(container, this, (Integer)this.properties.port.getValue(),(Integer)this.properties.sizeArray.getValue(),schema);
    }

    public ValidationResult validate(RuntimeContainer adaptor) {
        // Check that there is exactly one column to contain the output.
        if (schema.getFields().size() != 1) {
            ValidationResult vr = new ValidationResult();
            vr.setMessage("The schema must have exactly one column."); //$NON-NLS-1$
            vr.setStatus(ValidationResult.Result.ERROR);
            return vr;
        }
        
        return ValidationResult.OK;
    }

    public Schema getEndpointSchema(RuntimeContainer container, String schemaName) throws IOException {
        return null;
    }

    public List<NamedThing> getSchemaNames(RuntimeContainer container) throws IOException {
        return null;
    }

    public Schema getSchemaFromProperties(RuntimeContainer adaptor) throws IOException {
        return schema;
    }

    public Schema getPossibleSchemaFromProperties(RuntimeContainer adaptor) throws IOException {
        return schema;
    }

     public List<? extends BoundedSource> splitIntoBundles(long desiredBundleSizeBytes, RuntimeContainer adaptor) throws Exception {
        // There can be only one.
        return Arrays.asList(this);
     }

     public long getEstimatedSizeBytes(RuntimeContainer adaptor) {
        // This will be ignored since the source will never be split.
        return 0;
     }

     public boolean producesSortedKeys(RuntimeContainer adaptor) {
        return false;
     }

}