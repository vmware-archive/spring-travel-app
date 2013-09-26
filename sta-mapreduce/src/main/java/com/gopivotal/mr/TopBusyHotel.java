package com.gopivotal.mr;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.vmware.sqlfire.hadoop.mapred.Key;
import com.vmware.sqlfire.hadoop.mapred.Row;
import com.vmware.sqlfire.hadoop.mapred.RowInputFormat;
import com.vmware.sqlfire.hadoop.mapred.RowOutputFormat;
import com.vmware.sqlfire.internal.engine.SqlfDataSerializable;



public class TopBusyHotel extends Configured implements Tool {

  /**
   * Mapper used for first job. Produces tuples of the form:
   * 
   * MIA 1 JFK 1
   * 
   * This job is configured with a standard IntSumReducer to produce totals for
   * each airport code.
   */
  public static class SampleMapper extends MapReduceBase implements
      Mapper<Object, Row, LongWritable, IntWritable> {

    private final static IntWritable countOne = new IntWritable(1);

    @Override
    public void map(Object key, Row row,
        OutputCollector<LongWritable, IntWritable> output, Reporter reporter)
        throws IOException {

      long hotel_id;

      try {
        ResultSet rs = row.getRowAsResultSet();
        hotel_id = rs.getLong("HOTEL_ID");
        System.out.println("#####STA Received HOTEL_ID: " + hotel_id);
        output.collect(new LongWritable(hotel_id), countOne);
        System.out.println("#####STA Post Collect");
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  public static class SampleReducer extends MapReduceBase implements
      Reducer<LongWritable, IntWritable, Key, BusyHotelModel> {

    @Override
    public void reduce(LongWritable hotelId, Iterator<IntWritable> values,
        OutputCollector<Key, BusyHotelModel> output, Reporter reporter)
        throws IOException {
      int sum = 0;
      long hotel = hotelId.get();
      while (values.hasNext()) {
        sum += values.next().get();
      }
      System.out.println("##### SUM for hotel: " + hotelId + " count: "+ sum);
      output.collect(new Key(), new BusyHotelModel(hotel, sum));
      System.out.println("###POST Reduce");

    }
  }

 

  public int run(String[] args) throws Exception {

    SqlfDataSerializable.initTypes();

    JobConf conf = new JobConf(getConf());
    conf.setJobName("Busy Hotel Count");

    String hdfsHomeDir = "sta_tables";
    String tableName = "APP.BOOKING";

    conf.set(RowInputFormat.HOME_DIR, hdfsHomeDir);
    conf.set(RowInputFormat.INPUT_TABLE, tableName);
    conf.setBoolean(RowInputFormat.CHECKPOINT_MODE, false);

    conf.set(RowOutputFormat.OUTPUT_URL,
        "jdbc:sqlfire://kuwait.gemstone.com:1527");
    conf.set(RowOutputFormat.OUTPUT_TABLE, "APP.BUSY_HOTEL");
    conf.setOutputFormat(RowOutputFormat.class);
    
    conf.setInputFormat(RowInputFormat.class);
    conf.setMapperClass(SampleMapper.class);
    conf.setMapOutputKeyClass(LongWritable.class);
    conf.setMapOutputValueClass(IntWritable.class);

    conf.setReducerClass(SampleReducer.class);
    conf.setOutputKeyClass(Key.class);
    conf.setOutputValueClass(BusyHotelModel.class);


    int rc = JobClient.runJob(conf).isSuccessful() ? 0 : 1;
    return rc;
  }

  public static void main(String[] args) throws Exception {
    System.out.println("SampleApp.main() invoked with " + args);
    int rc = ToolRunner.run(new TopBusyHotel(), args);
    System.exit(rc);
  }
}
