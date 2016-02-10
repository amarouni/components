package org.talend.components.bd.api.component.spark;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.talend.components.api.exception.TalendConnectionException;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.api.runtime.input.Reader;
import org.talend.components.api.runtime.input.SingleSplit;
import org.talend.components.api.runtime.input.Source;
import org.talend.components.api.runtime.input.Split;
import org.talend.components.api.runtime.row.BaseRowStruct;
import org.talend.daikon.properties.Properties.Deserialized;
import org.talend.daikon.schema.SchemaElement;
import org.talend.daikon.schema.internal.DataSchemaElement;
import org.talend.daikon.schema.type.ExternalBaseType;
import org.talend.daikon.schema.type.TypeMapping;

/**
 * Created by bchen on 16-1-10.
 */
// TODO better to and a talend InputFormat to avoid the dependency of MapReduce, just need a method getSplits;no close
// method for InputFormat?
public class MRInputFormat implements InputFormat<NullWritable, BaseRowStruct>, JobConfigurable {

    private Source source;

    @Override
    public InputSplit[] getSplits(JobConf jobConf, int num) throws IOException {
        if (source.supportSplit()) {
            Split[] splits = source.getSplit(num);
            BDInputSplit[] bdInputSplits = new BDInputSplit[splits.length];
            for (int i = 0; i < bdInputSplits.length; i++) {
                bdInputSplits[i] = new BDInputSplit(splits[i]);
            }
            return bdInputSplits;
        } else {
            return new BDInputSplit[] { new BDInputSplit(new SingleSplit()) };
        }
    }

    @Override
    public RecordReader<NullWritable, BaseRowStruct> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter)
            throws IOException {
        try {
            return new BDRecordReader(source.getRecordReader(((BDInputSplit) inputSplit).getRealSplit()), source.getFamilyName());
        } catch (TalendConnectionException e) {
            throw new IOException(e.getMessage());
        }
    }

    @Override
    public void configure(JobConf jobConf) {
        try {
            Class<? extends Source> aClass = (Class<? extends Source>) Class.forName(jobConf.get("input.source"));
            this.source = aClass.newInstance();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        String componentPropertiesString = jobConf.get("input.props");
        Deserialized deserialized = org.talend.daikon.properties.Properties.fromSerialized(componentPropertiesString,
                ComponentProperties.class);
        ComponentProperties properties = (ComponentProperties) deserialized.properties;
        try {
            this.source.init(properties);
        } catch (TalendConnectionException e) {
            e.printStackTrace();
        }
    }

    static class BDRecordReader implements RecordReader<NullWritable, BaseRowStruct> {

        private Reader reader;

        private List<SchemaElement> schema;

        private String familyName;

        BDRecordReader(Reader reader, String familyName) {
            this.reader = reader;
            this.schema = reader.getSchema();
            this.familyName = familyName;
        }

        @Override
        public boolean next(NullWritable nullWritable, BaseRowStruct baseRowStruct) throws IOException {
            if (reader.advance()) {
                Object row = reader.getCurrent();

                for (SchemaElement column : schema) {
                    DataSchemaElement col = (DataSchemaElement) column;
                    try {
                        ExternalBaseType converter = col.getAppColType().newInstance();
                        Object dataValue = converter.convertToKnown(converter.readValue(row, col.getAppColName()));
                        baseRowStruct.put(col.getName(), TypeMapping.convert(familyName, col, dataValue));
                    } catch (InstantiationException e) {
                        e.printStackTrace();
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }
                }
                return true;
            } else {
                return false;
            }
        }

        @Override
        public NullWritable createKey() {
            return NullWritable.get();
        }

        @Override
        public BaseRowStruct createValue() {
            Map<String, SchemaElement.Type> row_metadata = new HashMap<>();
            for (SchemaElement field : schema) {
                row_metadata.put(field.getName(), field.getType());
            }
            return new BaseRowStruct(row_metadata);
        }

        @Override
        public long getPos() throws IOException {
            return 0;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        @Override
        public float getProgress() throws IOException {
            return 0;
        }
    }

    static class BDInputSplit implements InputSplit, Comparable<BDInputSplit> {

        private Split split;

        public BDInputSplit() {
            this(new SingleSplit());
        }

        public BDInputSplit(Split split) {
            this.split = split;
        }

        public Split getRealSplit() {
            return split;
        }

        @Override
        public int compareTo(BDInputSplit o) {
            return split.compareTo((Split) o);
        }

        @Override
        public long getLength() throws IOException {
            return split.getLength();
        }

        @Override
        public String[] getLocations() throws IOException {
            return split.getLocations();
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            split.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            split.readFields(dataInput);
        }
    }
}
