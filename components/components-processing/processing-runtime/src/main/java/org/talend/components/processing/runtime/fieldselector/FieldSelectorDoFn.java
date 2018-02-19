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
package org.talend.components.processing.runtime.fieldselector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Setup;
import org.apache.commons.lang3.StringUtils;
import org.talend.components.adapter.beam.kv.KeyValueUtils;
import org.talend.components.processing.definition.ProcessingErrorCode;
import org.talend.components.processing.definition.fieldselector.FieldSelectorProperties;
import org.talend.components.processing.definition.fieldselector.SelectorProperties;
import org.talend.components.processing.definition.filterrow.FilterRowProperties;
import org.talend.components.processing.definition.normalize.NormalizeProperties;
import org.talend.components.processing.runtime.normalize.NormalizeDoFn;

import scala.collection.JavaConversions;
import scala.util.Try;
import wandou.avpath.Evaluator;
import wandou.avpath.Parser;

public class FieldSelectorDoFn extends DoFn<IndexedRecord, IndexedRecord> {

    private FieldSelectorProperties properties = null;

    @Setup
    public void setup() throws Exception {
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        IndexedRecord inputRecord = context.element();

        Map<String, Object> selectedFields = new HashMap<>();
        List<Schema.Field> fieldSchemas = new ArrayList<>();
        for (SelectorProperties selector : properties.selectors.subProperties) {
            if (StringUtils.isNotEmpty(selector.path.getValue())) {
                List<Evaluator.Ctx> avPathContexts = getInputFields(inputRecord, selector.path.getValue());
                if (avPathContexts.size() > 0) {
                    if (isMultipleElements(avPathContexts, selector.path.getValue())) {
                        List<Object> toto = new ArrayList<>();
                        for (Evaluator.Ctx avPathcontext : avPathContexts) {
                            toto.add(avPathcontext.value());
                        }
                        if (toto.size() > 0) {
                            Evaluator.Ctx avPathcontext = avPathContexts.get(0);
                            fieldSchemas.add(new Field(selector.field.getValue(), //
                                    SchemaBuilder.array().items(avPathcontext.schema()), "", ""));
                            selectedFields.put(selector.field.getValue(), toto);
                        }
                    } else {
                        Evaluator.Ctx avPathcontext = avPathContexts.get(0);
                        fieldSchemas.add(new Field(selector.field.getValue(), avPathcontext.schema(), "", ""));
                        selectedFields.put(selector.field.getValue(), avPathcontext.value());
                    }
                }
            }
        }
        if (fieldSchemas.size() > 0) {
            Schema outputSchema = Schema.createRecord(fieldSchemas);
            GenericRecordBuilder outputRecordBuilder = new GenericRecordBuilder(outputSchema);
            for (Entry<String, Object> truc : selectedFields.entrySet()) {
                outputRecordBuilder.set(truc.getKey(), truc.getValue());
            }
    
            context.output(outputRecordBuilder.build());
        }
    }
    
    private boolean isMultipleElements(List<Evaluator.Ctx> avPathContexts, String path) {
        return avPathContexts.size() > 1 || path.contains("{") || path.contains("[") || path.contains("\\.\\.");
    }

    private List<Evaluator.Ctx> getInputFields(IndexedRecord inputRecord, String columnName) {
        // Adapt non-avpath syntax to avpath.
        // TODO: This should probably not be automatic, use the actual syntax.
        if (!columnName.startsWith("."))
            columnName = "." + columnName;
        Try<scala.collection.immutable.List<Evaluator.Ctx>> result =
                wandou.avpath.package$.MODULE$.select(inputRecord, columnName);
        List<Evaluator.Ctx> values = new ArrayList<Evaluator.Ctx>();
        if (result.isSuccess()) {
            for (Evaluator.Ctx ctx : JavaConversions.asJavaCollection(result.get())) {
                values.add(ctx);
            }
        } else {
            // Evaluating the expression failed, and we can handle the exception.
            throw ProcessingErrorCode.createAvpathSyntaxError(result.failed().get(), columnName, -1);
        }
        return values;
    }

    public FieldSelectorDoFn withProperties(FieldSelectorProperties properties) {
        this.properties = properties;
        return this;
    }
}
