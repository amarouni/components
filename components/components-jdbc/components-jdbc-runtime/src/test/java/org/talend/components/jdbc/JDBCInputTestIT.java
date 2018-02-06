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
package org.talend.components.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.talend.components.api.component.ComponentDefinition;
import org.talend.components.api.component.runtime.Reader;
import org.talend.components.api.container.DefaultComponentRuntimeContainerImpl;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.api.exception.ComponentException;
import org.talend.components.jdbc.common.DBTestUtils;
import org.talend.components.jdbc.runtime.JDBCSource;
import org.talend.components.jdbc.runtime.JdbcRuntimeUtils;
import org.talend.components.jdbc.runtime.reader.JDBCInputReader;
import org.talend.components.jdbc.runtime.setting.AllSetting;
import org.talend.components.jdbc.tjdbcinput.TJDBCInputDefinition;
import org.talend.components.jdbc.tjdbcinput.TJDBCInputProperties;
import org.talend.daikon.NamedThing;
import org.talend.daikon.avro.converter.IndexedRecordConverter;

public class JDBCInputTestIT {

    public static AllSetting allSetting;

    @BeforeClass
    public static void beforeClass() throws Exception {
        allSetting = DBTestUtils.createAllSetting();

        try (Connection conn = JdbcRuntimeUtils.createConnection(allSetting)) {
            DBTestUtils.createTestTable(conn, tablename);
            DBTestUtils.createAllTypesTable(conn, tablename_all_type);
        }
    }

    private static final String tablename = "JDBCINPUT";

    private static final String tablename_all_type = "JDBCINPUTALLTYPE";

    @AfterClass
    public static void afterClass() throws ClassNotFoundException, SQLException {
        try (Connection conn = JdbcRuntimeUtils.createConnection(allSetting)) {
            DBTestUtils.dropTestTable(conn, tablename);
            DBTestUtils.dropAllTypesTable(conn, tablename_all_type);
        } finally {
            DBTestUtils.shutdownDBIfNecessary();
        }
    }

    @Before
    public void before() throws SQLException, ClassNotFoundException {
        try (Connection conn = JdbcRuntimeUtils.createConnection(allSetting)) {
            DBTestUtils.truncateTable(conn, tablename);
            DBTestUtils.loadTestData(conn, tablename);
            DBTestUtils.truncateAllTypesTable(conn, tablename_all_type);
            DBTestUtils.loadAllTypesData(conn, tablename_all_type);
        }
    }

    @Test
    public void testGetSchemaNames() throws Exception {
        TJDBCInputDefinition definition = new TJDBCInputDefinition();
        TJDBCInputProperties properties = DBTestUtils.createCommonJDBCInputProperties(allSetting, definition);

        properties.main.schema.setValue(DBTestUtils.createTestSchema(tablename));
        properties.tableSelection.tablename.setValue(tablename);
        properties.sql.setValue(DBTestUtils.getSQL(tablename));

        JDBCSource source = DBTestUtils.createCommonJDBCSource(properties);

        List<NamedThing> schemaNames = source.getSchemaNames(null);
        assertTrue(schemaNames != null);
        assertTrue(!schemaNames.isEmpty());

        boolean exists = false;
        for (NamedThing name : schemaNames) {
            if (tablename.equals(name.getName().toUpperCase())) {
                exists = true;
                break;
            }
        }

        assertTrue(exists);
    }

    @Test(expected = ComponentException.class)
    public void testGetSchemaNamesWithException() throws Exception {
        TJDBCInputDefinition definition = new TJDBCInputDefinition();
        TJDBCInputProperties properties = DBTestUtils.createCommonJDBCInputProperties(allSetting, definition);

        properties.connection.driverClass.setValue("notexist");

        JDBCSource source = DBTestUtils.createCommonJDBCSource(properties);

        source.getSchemaNames(null);
    }

    @Test(expected = ComponentException.class)
    public void testGetSchemaWithException() throws Exception {
        TJDBCInputDefinition definition = new TJDBCInputDefinition();
        TJDBCInputProperties properties = DBTestUtils.createCommonJDBCInputProperties(allSetting, definition);

        properties.connection.driverClass.setValue("notexist");

        JDBCSource source = DBTestUtils.createCommonJDBCSource(properties);

        source.getEndpointSchema(null, tablename);
    }

    @Test(expected = ComponentException.class)
    public void testGetSchemaFromQueryWithException1() throws Exception {
        TJDBCInputDefinition definition = new TJDBCInputDefinition();
        TJDBCInputProperties properties = DBTestUtils.createCommonJDBCInputProperties(allSetting, definition);

        JDBCSource source = DBTestUtils.createCommonJDBCSource(properties);

        source.getSchemaFromQuery(null, "select * from notexist");
    }

    @Test(expected = ComponentException.class)
    public void testGetSchemaFromQueryWithException2() throws Exception {
        TJDBCInputDefinition definition = new TJDBCInputDefinition();
        TJDBCInputProperties properties = DBTestUtils.createCommonJDBCInputProperties(allSetting, definition);

        properties.connection.driverClass.setValue("notexist");

        JDBCSource source = DBTestUtils.createCommonJDBCSource(properties);

        source.getSchemaFromQuery(null, "select * from " + tablename);
    }

    @Test(expected = ComponentException.class)
    public void testGetSchemaFromQueryWithException3() throws Exception {
        TJDBCInputDefinition definition = new TJDBCInputDefinition();
        TJDBCInputProperties properties = DBTestUtils.createCommonJDBCInputProperties(allSetting, definition);

        properties.connection.jdbcUrl.setValue("wrongone");

        JDBCSource source = DBTestUtils.createCommonJDBCSource(properties);

        source.getSchemaFromQuery(null, "select * from " + tablename);
    }

    @Test
    public void testGetSchema() throws Exception {
        TJDBCInputDefinition definition = new TJDBCInputDefinition();
        TJDBCInputProperties properties = DBTestUtils.createCommonJDBCInputProperties(allSetting, definition);

        properties.main.schema.setValue(DBTestUtils.createTestSchema(tablename));
        properties.tableSelection.tablename.setValue(tablename);
        properties.sql.setValue(DBTestUtils.getSQL(tablename));

        JDBCSource source = DBTestUtils.createCommonJDBCSource(properties);

        RuntimeContainer container = new DefaultComponentRuntimeContainerImpl() {

            @Override
            public String getCurrentComponentId() {
                return "tJDBCInput1";
            }
        };
        
        // the getResource method will convert "@" to "%40" when work with maven together, not sure the bug appear where, bug make
        // sure it come from the env, not the function code, so only convert here
        // in the product env, the mappings_url is passed from the platform
        java.net.URL mappings_url = this.getClass().getResource("/mappings");
        mappings_url = DBTestUtils.correctURL(mappings_url);
        
        container.setComponentData(container.getCurrentComponentId(), ComponentConstants.MAPPING_URL_SUBFIX,
                mappings_url);

        Schema schema = source.getEndpointSchema(container, tablename);
        assertEquals(tablename, schema.getName().toUpperCase());
        List<Field> columns = schema.getFields();
        DBTestUtils.testMetadata(columns);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testReader() {
        Reader reader = null;
        try {
            TJDBCInputDefinition definition = new TJDBCInputDefinition();
            TJDBCInputProperties properties = DBTestUtils.createCommonJDBCInputProperties(allSetting, definition);

            properties.main.schema.setValue(DBTestUtils.createTestSchema(tablename));
            properties.tableSelection.tablename.setValue(tablename);
            properties.sql.setValue(DBTestUtils.getSQL(tablename));

            reader = DBTestUtils.createCommonJDBCInputReader(properties);

            reader.start();

            IndexedRecord row = (IndexedRecord) reader.getCurrent();
            Integer id = (Integer) row.get(0);
            String name = (String) row.get(1);

            assertEquals(1, id.intValue());
            assertEquals("wangwei", name);

            reader.advance();

            row = (IndexedRecord) reader.getCurrent();
            id = (Integer) row.get(0);
            name = (String) row.get(1);

            assertEquals(2, id.intValue());
            assertEquals("gaoyan", name);

            reader.advance();

            row = (IndexedRecord) reader.getCurrent();
            id = (Integer) row.get(0);
            name = (String) row.get(1);

            assertEquals(3, id.intValue());
            assertEquals("dabao", name);

            reader.close();

            Map<String, Object> returnMap = reader.getReturnValues();
            Assert.assertEquals(3, returnMap.get(ComponentDefinition.RETURN_TOTAL_RECORD_COUNT));
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    Assert.fail(e.getMessage());
                }
            }
        }

    }

    /**
     * Checks {@link JDBCInputReader} outputs {@link IndexedRecord} which contains nullable String type data for every SQL/JDBC
     * type
     */
    @Test
    public void testReaderAllTypesString() throws IOException {
        TJDBCInputDefinition definition = new TJDBCInputDefinition();
        TJDBCInputProperties properties = DBTestUtils.createCommonJDBCInputProperties(allSetting, definition);

        properties.main.schema.setValue(DBTestUtils.createAllTypesSchema(tablename_all_type));
        properties.tableSelection.tablename.setValue(tablename_all_type);
        properties.sql.setValue(DBTestUtils.getSQL(tablename_all_type));

        Reader reader = DBTestUtils.createCommonJDBCInputReader(properties);

        reader.start();

        IndexedRecord record = (IndexedRecord) reader.getCurrent();
        Short col0 = (Short) record.get(0);
        Integer col1 = (Integer) record.get(1);
        Long col2 = (Long) record.get(2);
        Float col3 = (Float) record.get(3);
        Double col4 = (Double) record.get(4);
        BigDecimal col5 = (BigDecimal) record.get(5);
        String col6 = (String) record.get(6);
        String col7 = (String) record.get(7);
        String col8 = (String) record.get(8);
        String col9 = (String) record.get(9);
        Long col10 = (Long) record.get(10);
        Long col11 = (Long) record.get(11);
        Long col12 = (Long) record.get(12);
        Boolean col13 = (Boolean) record.get(13);

        assertEquals(32767, col0.shortValue());
        assertEquals(2147483647, col1.intValue());
        assertEquals(9223372036854775807l, col2.longValue());
        assertTrue(col3 > 1);
        assertTrue(col4 > 2);
        assertEquals(new BigDecimal("1234567890.1234567890"), col5);
        assertEquals("abcd", col6);
        assertEquals("abcdefg", col7);
        assertEquals("00010203040506070809", col8);
        assertEquals("abcdefg", col9);
        assertEquals("2016-12-28", new SimpleDateFormat("yyyy-MM-dd").format(new Date(col10)));
        assertEquals("14:30:33", new SimpleDateFormat("HH:mm:ss").format(new Date(col11)));
        assertEquals("2016-12-28 14:31:56.123", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(col12)));
        assertEquals(true, col13);

        Schema actualSchema = record.getSchema();
        List<Field> actualFields = actualSchema.getFields();

        assertEquals(14, actualFields.size());
        reader.close();
    }

    @SuppressWarnings({ "rawtypes" })
    @Test
    public void testType() throws Exception {
        TJDBCInputDefinition definition = new TJDBCInputDefinition();
        TJDBCInputProperties properties = DBTestUtils.createCommonJDBCInputProperties(allSetting, definition);

        properties.main.schema.setValue(DBTestUtils.createTestSchema(tablename));
        properties.tableSelection.tablename.setValue(tablename);
        properties.sql.setValue(DBTestUtils.getSQL(tablename));

        Reader reader = DBTestUtils.createCommonJDBCInputReader(properties);

        try {
            IndexedRecordConverter<Object, ? extends IndexedRecord> converter = null;

            for (boolean available = reader.start(); available; available = reader.advance()) {
                converter = DBTestUtils.getIndexRecordConverter(reader, converter);

                IndexedRecord record = converter.convertToAvro(reader.getCurrent());

                assertEquals(Integer.class, record.get(0).getClass());
                assertEquals(String.class, record.get(1).getClass());
            }

            reader.close();
        } finally {
            reader.close();
        }
    }

}
