package org.talend.components.processing.definition.fieldselector;

import org.junit.Test;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.serialize.jsonschema.JsonSchemaUtil;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.notNullValue;

public class FieldSelectorPropertiesTest {

    @Test
    public void testDefaultProperties() {
        FieldSelectorProperties properties = new FieldSelectorProperties("test");
        assertThat(properties.selectors.getDefaultProperties(), notNullValue());
        assertThat(properties.getAllSchemaPropertiesConnectors(true), contains(properties.OUTGOING_CONNECTOR));
        assertThat(properties.getAllSchemaPropertiesConnectors(false), contains(properties.INCOMING_CONNECTOR));
        System.out.println(JsonSchemaUtil.toJson(properties, Form.MAIN, "FieldSelectorDefinition"));
        
    }
}
