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

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.*;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.talend.components.processing.runtime.SampleAvpathSchemas.SyntheticDatasets.getSubrecords;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.talend.components.processing.definition.fieldselector.FieldSelectorProperties;
import org.talend.components.processing.definition.fieldselector.SelectorProperties;
import org.talend.components.processing.runtime.SampleAvpathSchemas;
import org.talend.daikon.exception.TalendRuntimeException;

public class FieldSelectorDoFnTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private final Schema inputSimpleSchema = SchemaBuilder
            .record("inputRow") //
            .fields() //
            .name("a")
            .type()
            .optional()
            .stringType() //
            .name("b")
            .type()
            .optional()
            .stringType() //
            .name("c")
            .type()
            .optional()
            .stringType() //
            .endRecord();

    private final GenericRecord inputSimpleRecord = new GenericRecordBuilder(inputSimpleSchema) //
            .set("a", "aaa") //
            .set("b", "BBB") //
            .set("c", "Ccc") //
            .build();

    private final IndexedRecord inputHierarchical = SampleAvpathSchemas.SyntheticDatasets.getRandomRecord(new Random(0),
            SampleAvpathSchemas.SyntheticDatasets.RECORD_A);

    private final IndexedRecord[] inputB = SampleAvpathSchemas.SyntheticDatasets.getRandomRecords(1000, new Random(0),
            SampleAvpathSchemas.SyntheticDatasets.RECORD_B);

    protected static FieldSelectorProperties addSelector(FieldSelectorProperties fsp, String field, String path) {
        // Create a new properties if one wasn't passed in.
        if (fsp == null) {
            fsp = new FieldSelectorProperties("test");
            fsp.init();
            // Remove the default critera.
            fsp.selectors.subProperties.clear();
        }

        // Create and add a new selector with the requested properties.
        SelectorProperties selector = new SelectorProperties("filter");
        selector.init();
        fsp.selectors.addRow(selector);

        if (field != null)
            selector.field.setValue(field);
        if (path != null)
            selector.path.setValue(path);

        return fsp;
    }

    /**
     * When there are no user input, the component not return any data
     */
    @Test
    public void noSelector() throws Exception {
        // Create a filter row with exactly one criteria that hasn't been filled by the user.
        FieldSelectorProperties properties = addSelector(null, null, null);
        assertThat(properties.selectors.getPropertiesList(), hasSize(1));

        SelectorProperties selector = properties.selectors.getPropertiesList().iterator().next();
        assertThat(selector.field.getStringValue(), is(""));
        assertThat(selector.path.getStringValue(), is(""));

        FieldSelectorDoFn function = new FieldSelectorDoFn().withProperties(properties);
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);

        //
        List<IndexedRecord> outputs = fnTester.processBundle(inputSimpleRecord);
        assertEquals(0, outputs.size());
    }

    @Test
    public void selectSimpleElement() throws Exception {
        FieldSelectorProperties properties = addSelector(null, "aOutput", "a");

        FieldSelectorDoFn function = new FieldSelectorDoFn().withProperties(properties);
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        List<IndexedRecord> outputs = fnTester.processBundle(inputSimpleRecord);

        assertEquals(1, outputs.size());
        List<Field> fields = outputs.get(0).getSchema().getFields();
        assertEquals(1, fields.size());
        assertEquals("aOutput", fields.get(0).name());
        assertEquals("aaa", outputs.get(0).get(0));
    }

    @Test
    public void selectSimpleElements() throws Exception {
        FieldSelectorProperties properties = addSelector(null, "aOutput", "a");
        properties = addSelector(properties, "cOutput", "c");
        properties = addSelector(properties, "aSecondOutput", "a");
        properties = addSelector(properties, "bOutput", "b");

        FieldSelectorDoFn function = new FieldSelectorDoFn().withProperties(properties);
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        List<IndexedRecord> outputs = fnTester.processBundle(inputSimpleRecord);

        assertEquals(1, outputs.size());
        List<Field> fields = outputs.get(0).getSchema().getFields();
        assertEquals(4, fields.size());
        assertEquals("aOutput", fields.get(0).name());
        assertEquals("aaa", outputs.get(0).get(0));
        assertEquals("cOutput", fields.get(1).name());
        assertEquals("Ccc", outputs.get(0).get(1));
        assertEquals("aSecondOutput", fields.get(2).name());
        assertEquals("aaa", outputs.get(0).get(2));
        assertEquals("bOutput", fields.get(3).name());
        assertEquals("BBB", outputs.get(0).get(3));
    }

    @Test
    public void testBasicHierarchical() throws Exception {
        FieldSelectorProperties properties = addSelector(null, "id", ".id");
        properties = addSelector(properties, "name", ".a1.name");
        properties = addSelector(properties, "subname", ".a1.a2.name");

        FieldSelectorDoFn function = new FieldSelectorDoFn().withProperties(properties);
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        List<IndexedRecord> outputs = fnTester.processBundle(inputHierarchical);

        assertEquals(1, outputs.size());
        List<Field> fields = outputs.get(0).getSchema().getFields();
        assertEquals(3, fields.size());
        assertEquals("id", fields.get(0).name());
        assertEquals(1, outputs.get(0).get(0));
        assertEquals("name", fields.get(1).name());
        assertEquals("P8A933FLOC", outputs.get(0).get(1));
        assertEquals("subname", fields.get(2).name());
        assertEquals("Q2G5V64PQQ", outputs.get(0).get(2));
    }

    @Test
    public void testHierarchicalWithSelector() throws Exception {
        FieldSelectorProperties properties =
                addSelector(null, "yearOfToyota", ".automobiles{.maker === \"Toyota\"}.year");
        IndexedRecord input = SampleAvpathSchemas.Vehicles.getDefaultVehicleCollection();

        FieldSelectorDoFn function = new FieldSelectorDoFn().withProperties(properties);
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        List<IndexedRecord> outputs = fnTester.processBundle(input);

        assertEquals(1, outputs.size());
        List<Field> fields = outputs.get(0).getSchema().getFields();
        assertEquals(1, fields.size());
        assertEquals("yearOfToyota", fields.get(0).name());
        assertThat(((List<Integer>) outputs.get(0).get(0)), hasItems(2016, 2017));
    }

    @Test
    public void testHierarchicalUnknownColumn() throws Exception {
        FieldSelectorProperties properties = addSelector(null, "id", ".unknow");

        FieldSelectorDoFn function = new FieldSelectorDoFn().withProperties(properties);
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);

        List<IndexedRecord> outputs = fnTester.processBundle(inputHierarchical);

        // None of the records can possibly match.
        // TODO(TFD-2194): This should throw an exception if possible.
        // Until that is the case, there should be no output
        assertEquals(0, outputs.size());
    }

    @Test(expected = TalendRuntimeException.class)
    public void testHierarchicalSyntaxError() throws Exception {
        FieldSelectorProperties properties = addSelector(null, "id", "asdf&*{.\\\\t");

        FieldSelectorDoFn function = new FieldSelectorDoFn().withProperties(properties);
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        List<IndexedRecord> outputs = fnTester.processBundle(inputHierarchical);
    }

    @Test
    public void testHierarchicalSubRecordHasValueGt10() throws Exception {
        FieldSelectorProperties properties = addSelector(null, "value", ".b1{.value > 10}");
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of( //
                new FieldSelectorDoFn().withProperties(properties));

        List<IndexedRecord> output = fnTester.processBundle(inputB);
        for (IndexedRecord main : output) {
            List<Field> fields = main.getSchema().getFields();
            assertEquals(1, fields.size());
            assertEquals("value", fields.get(0).name());
            for (IndexedRecord element : (List<IndexedRecord>) main.get(0)) {
                List<Field> subFields = element.getSchema().getFields();
                assertEquals("id", subFields.get(0).name());
                assertEquals("name", subFields.get(1).name());
                assertEquals("value", subFields.get(2).name());
                assertEquals("b2", subFields.get(3).name());
                assertThat((Double) element.get(2), greaterThan(10d));
            }
        }
    }

    @Test
    public void testHierarchicalAllSubRecordsHaveValueGt10() throws Exception {
        FieldSelectorProperties properties = addSelector(null, "value", ".b1{.value <= 10}");
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of( //
                new FieldSelectorDoFn().withProperties(properties));

        List<IndexedRecord> output = fnTester.processBundle(inputB);
        for (IndexedRecord main : output) {
            List<Field> fields = main.getSchema().getFields();
            assertEquals(1, fields.size());
            assertEquals("value", fields.get(0).name());
            for (IndexedRecord element : (List<IndexedRecord>) main.get(0)) {
                assertThat((Integer) element.get(0), lessThanOrEqualTo(10));
            }
        }
    }

    @Test
    public void testHierarchicalFirstRecordValue() throws Exception {
        FieldSelectorProperties properties = addSelector(null, "value", ".b1[0].value");
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of( //
                new FieldSelectorDoFn().withProperties(properties));

        List<IndexedRecord> output = fnTester.processBundle(inputB);
        for (IndexedRecord main : output) {
            List<Field> fields = main.getSchema().getFields();
            assertEquals(1, fields.size());
            assertEquals("value", fields.get(0).name());
        }
    }

    @Test
    public void testHierarchicalLastRecordValue() throws Exception {
        FieldSelectorProperties properties = addSelector(null, "value", ".b1[-1].value");
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of( //
                new FieldSelectorDoFn().withProperties(properties));

        List<IndexedRecord> output = fnTester.processBundle(inputB);
        for (IndexedRecord main : output) {
            List<Field> fields = main.getSchema().getFields();
            assertEquals(1, fields.size());
            assertEquals("value", fields.get(0).name());
        }
    }

    @Test
    public void testHierarchicalSubRecordsWithId1Or2HasValueGt10_Alternative() throws Exception {
        FieldSelectorProperties properties =
                addSelector(null, "expectedb1", ".b1{.id == 1 && .value > 10 || .id == 2 && .value > 10}");
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of( //
                new FieldSelectorDoFn().withProperties(properties));

        List<IndexedRecord> output = fnTester.processBundle(inputB);
        for (IndexedRecord main : output) {
            boolean atLeastOne = false;
            for (IndexedRecord subrecord : getSubrecords(main)) {
                int id = (int) subrecord.get(0);
                if ((double) subrecord.get(2) > 10 && (id == 1 || id == 2))
                    atLeastOne = true;
            }
            if (atLeastOne) {
                List<Field> fields = main.getSchema().getFields();
                assertEquals(1, fields.size());
                assertEquals("expectedb1", fields.get(0).name());
                List<IndexedRecord> subElements = (List<IndexedRecord>) main.get(0);
                for (IndexedRecord subElement: subElements) {
                    List<Field> subFields = subElement.getSchema().getFields();
                    assertEquals(4, subFields.size());
                    assertEquals("id", subFields.get(0).name());
                    assertEquals("name", subFields.get(1).name());
                    assertEquals("value", subFields.get(2).name());
                    assertEquals("b2", subFields.get(3).name());
    
                    assertThat((Integer) subElement.get(0), isOneOf(1, 2));
                    assertThat((Double) subElement.get(2), greaterThan(10d));
                }
            } else {
                assertFalse("You whould not create elements when there is not items", true);
            }
        }
    }

    @Test
    public void testHierarchicalSubrecordWithSubSubRecordValueGt10() throws Exception {
        FieldSelectorProperties properties = addSelector(null, "expectedb1", ".b1{.b2.value > 10}");
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of( //
                new FieldSelectorDoFn().withProperties(properties));

        List<IndexedRecord> output = fnTester.processBundle(inputB);
        
        
        for (IndexedRecord main : output) {
            boolean atLeastOne = false;
            for (IndexedRecord subrecord : getSubrecords(main)) {
                for (IndexedRecord subsubrecord : getSubrecords(subrecord)) {
                    if ((double) subsubrecord.get(2) > 10)
                        atLeastOne = true;
                }
            }
            if (atLeastOne) {
                List<Field> fields = main.getSchema().getFields();
                assertEquals(1, fields.size());
                assertEquals("expectedb1", fields.get(0).name());
                List<IndexedRecord> subElements = (List<IndexedRecord>) main.get(0);
                for (IndexedRecord subElement: subElements) {
                    List<Field> subFields = subElement.getSchema().getFields();
                    assertEquals(4, subFields.size());
                    assertEquals("id", subFields.get(0).name());
                    assertEquals("name", subFields.get(1).name());
                    assertEquals("value", subFields.get(2).name());
                    assertEquals("b2", subFields.get(3).name());
    
                    IndexedRecord subSubElement = (IndexedRecord) subElement.get(3);
                    List<Field> subSubFields = subSubElement.getSchema().getFields();
                    assertEquals(3, subSubFields.size());
                    assertEquals("id", subSubFields.get(0).name());
                    assertEquals("name", subSubFields.get(1).name());
                    assertEquals("value", subSubFields.get(2).name());
    
                    assertThat((Double) subSubElement.get(2), greaterThan(10d));
                }
            } else {
                assertFalse("You whould not create elements when there is not items", true);
            }
        }
    }

}
