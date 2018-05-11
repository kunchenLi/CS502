package com.example.pig;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.nutch.parse.ParseData;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.Expression;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;

public class NutchParsedDataLoader extends FileInputLoadFunc implements LoadMetadata {

    private SequenceFileRecordReader<Writable, Writable> reader;
    
    private Text key;
    private ParseData value;
    
    protected static final Log LOG = LogFactory.getLog(NutchParsedDataLoader.class);
    protected TupleFactory mTupleFactory = TupleFactory.getInstance();
      
    public NutchParsedDataLoader() {
    }
    
    @Override
    public Tuple getNext() throws IOException {
      boolean next = false;
      try {
        next = reader.nextKeyValue();
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
      
      if (!next) return null;
      
      key = (Text)reader.getCurrentKey();
      value = (ParseData)reader.getCurrentValue();

      Tuple t =  mTupleFactory.newTuple(14);
      t.set(0, key.toString());
      t.set(1, value.getTitle());
      t.set(2, value.getMeta("name"));
      t.set(3, value.getMeta("publisher"));
      t.set(4, value.getMeta("updateTime"));
      t.set(5, value.getMeta("category"));
      t.set(6, value.getMeta("price"));
      t.set(7, value.getMeta("reviewScore"));
      t.set(8, value.getMeta("reviewCount"));
      t.set(9, value.getMeta("install"));
      t.set(10, value.getMeta("version"));
      t.set(11, value.getMeta("rating"));
      t.set(12, value.getMeta("developerSite"));
      t.set(13, value.getMeta("developerEmail"));
      
      return t;
    }

    @SuppressWarnings("unchecked")
    @Override
    public InputFormat getInputFormat() throws IOException {
      return new SequenceFileInputFormat<Writable, Writable>();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split)
          throws IOException {
      this.reader = (SequenceFileRecordReader) reader;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
      FileInputFormat.setInputPaths(job, location);    
    }

    @Override
    public String[] getPartitionKeys(String location, Job job) throws IOException {
        return null;
    }

    @Override
    public ResourceSchema getSchema(String location, Job job) throws IOException {
        ResourceSchema schema = new ResourceSchema();
        ResourceFieldSchema fields[] = new ResourceFieldSchema[14];
        fields[0] = new ResourceFieldSchema(); fields[0].setName("url"); fields[0].setType(DataType.CHARARRAY);
        fields[1] = new ResourceFieldSchema(); fields[1].setName("title"); fields[1].setType(DataType.CHARARRAY);
        fields[2] = new ResourceFieldSchema(); fields[2].setName("name"); fields[2].setType(DataType.CHARARRAY);
        fields[3] = new ResourceFieldSchema(); fields[3].setName("publisher"); fields[3].setType(DataType.CHARARRAY);
        fields[4] = new ResourceFieldSchema(); fields[4].setName("updateTime"); fields[4].setType(DataType.CHARARRAY);
        fields[5] = new ResourceFieldSchema(); fields[5].setName("category"); fields[5].setType(DataType.CHARARRAY);
        fields[6] = new ResourceFieldSchema(); fields[6].setName("price"); fields[6].setType(DataType.CHARARRAY);
        fields[7] = new ResourceFieldSchema(); fields[7].setName("reviewScore"); fields[7].setType(DataType.CHARARRAY);
        fields[8] = new ResourceFieldSchema(); fields[8].setName("reviewCount"); fields[8].setType(DataType.CHARARRAY);
        fields[9] = new ResourceFieldSchema(); fields[9].setName("install"); fields[9].setType(DataType.CHARARRAY);
        fields[10] = new ResourceFieldSchema(); fields[10].setName("version"); fields[10].setType(DataType.CHARARRAY);
        fields[11] = new ResourceFieldSchema(); fields[11].setName("rating"); fields[11].setType(DataType.CHARARRAY);
        fields[12] = new ResourceFieldSchema(); fields[12].setName("developerSite"); fields[12].setType(DataType.CHARARRAY);
        fields[13] = new ResourceFieldSchema(); fields[13].setName("developerEmail"); fields[13].setType(DataType.CHARARRAY);
        schema.setFields(fields);
        return schema;
    }

    @Override
    public ResourceStatistics getStatistics(String location, Job job)
            throws IOException {
        return null;
    }

    @Override
    public void setPartitionFilter(Expression expr) throws IOException {
    }
}
