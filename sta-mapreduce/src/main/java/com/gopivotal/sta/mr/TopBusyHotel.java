package com.gopivotal.sta.mr;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Task.Counter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.gopivotal.sta.model.BusyHotelModel;
import com.vmware.sqlfire.hadoop.mapred.Key;
import com.vmware.sqlfire.hadoop.mapred.Row;
import com.vmware.sqlfire.hadoop.mapred.RowInputFormat;
import com.vmware.sqlfire.hadoop.mapred.RowOutputFormat;
//import com.vmware.sqlfire.internal.engine.SqlfDataSerializable;


public class TopBusyHotel extends Configured implements Tool {

  /**
   * Mapper used for first job. Produces tuples of the form:
   * 
   * <HOTEL_ID> 1 
   * 
   * This job is configured with a standard IntSumReducer to produce totals for
   * each Hotel booking.
   */
  public static class HotelFinder implements Mapper<Object, Row, LongWritable, IntWritable> {

    private final static IntWritable countOne = new IntWritable(1);
    
    @Override
    public void map(Object key, Row row,
        OutputCollector<LongWritable, IntWritable> output, Reporter reporter)
        throws IOException {

      long hotel_id;

      try {
        ResultSet rs = row.getRowAsResultSet();
        hotel_id = rs.getLong("HOTEL_ID");
        output.collect(new LongWritable(hotel_id), countOne);
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }


	@Override
	public void configure(JobConf arg0) {
	}


	@Override
	public void close() throws IOException {
		
	}
	
  }
  
  /***
   * Sums the <HOTEL_ID> 1 from the mapper to get the count of bookings for a hotel.
   * At the end of this reduce you have 
   * <HOTEL_ID> Count  
   * This reducer writes the hotel_id and its count to an intermediate file as an output.
   * @author bansods
   */
  public static class HotelCounter  implements Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {

    @Override
    public void reduce(LongWritable hotelId, Iterator<IntWritable> values,
        OutputCollector<LongWritable, IntWritable> output, Reporter reporter)
        throws IOException {
      int sum = 0;
      long hotel = hotelId.get();
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(new LongWritable(hotel), new IntWritable(sum));
    }
    
    
	@Override
	public void configure(JobConf arg0) {
		
	}

	@Override
	public void close() throws IOException {
		
	}
  }
  
 /**
  * Inverts <HOTEL_ID> <COUNT>
  * The output of mapper is <COUNT> , <HOTEL_ID>
  * This is done to so that the reducer will sort on the count of hotels.
  * @author bansods
  *
  */
  public static class HotelCountInverter implements Mapper<LongWritable, Text, IntWritable, LongWritable> {

	@Override
	public void configure(JobConf conf) {
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public void map(LongWritable key, Text values,
			OutputCollector<IntWritable, LongWritable> output, Reporter reporter)
			throws IOException {
		String valuesStr = values.toString();
		String [] splits = valuesStr.split("\\s+");
		
		IntWritable bookings = new IntWritable(Integer.parseInt(splits[1]));
		LongWritable hotelId = new LongWritable(Long.parseLong(splits[0]));
		output.collect(bookings, hotelId);
	}
  }
  
  /****
   * Contains the logic to update the price of a Hotel based on the number of bookings.
   * @author bansods
   *
   */
  public static class HotelPriceUpdater implements Reducer<IntWritable, LongWritable, Key, BusyHotelModel> {
	private static final Map<Long, BusyHotelModel> hotelMap = new HashMap<Long, BusyHotelModel>();
	private static int counter = 0;
	private static long numHotels = 0;
	private static int factor = 10;
	private static Connection connection = null;
	private static String connectionString = null;
	
	@Override
	public void configure(JobConf conf) {
		numHotels = conf.getLong("NumHotels", -1);
		try {
			//connection =  DriverManager.getConnection("jdbc:sqlfire://kuwait.gemstone.com:1527/");
			connectionString = conf.get("sqlf-connection-string");
			connection =  DriverManager.getConnection(connectionString);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		hotelMap.putAll(getHotelInfo());
	}

	@Override
	public void close() throws IOException {
		hotelMap.clear();
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void reduce(IntWritable bookings, Iterator<LongWritable> hotelIds,
			OutputCollector<Key, BusyHotelModel> output, Reporter reporter)
			throws IOException {
		
		while (hotelIds.hasNext()) {
			long hotelId = hotelIds.next().get();
			counter++;
			//if the hotel is in the lower half then decrease the value by factor percent
			
			BusyHotelModel hotel = hotelMap.get(hotelId);
			double price = hotel.getPrice();
			System.out.println("Hotel Id : " + hotelId + " Name : " + hotel.getName() + " Old Price : " + hotel.getPrice());

			if (counter < numHotels/2) {
				price = price - ((price * factor)/100.0); 
			}else {
				price = price + ((price * factor)/100.0); 
			}
			hotel.setNewPrice(price);
			System.out.println("Hotel Id : " + hotelId + " Name : " + hotel.getName() + " New Price : " + hotel.getPrice());
			output.collect(new Key(), hotel);
		}
	}
  }
	public static Map<Long, BusyHotelModel>  getHotelInfo () {
		final Map<Long, BusyHotelModel> hotelMap = new HashMap<Long, BusyHotelModel>();
		try {
			Statement stmt = HotelPriceUpdater.connection.createStatement();
			ResultSet rs = stmt.executeQuery("Select * from Hotel");
			
			while (rs.next()) {
				long hotelId = rs.getLong("id");
				String address = rs.getString("Address");
				String city = rs.getString("City");
				String country = rs.getString("Country");
				String name = rs.getString("Name");
				String state = rs.getString("State");
				String zip = rs.getString("Zip");
				float price = rs.getFloat("Price");
				hotelMap.put(hotelId, new BusyHotelModel(hotelId, address, price, city, country, name, state, zip));
			}
			rs.close();
		} catch (SQLException ex) {
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());
		} 
		return hotelMap;
	}

  public int run(String[] args) throws Exception {
	String sqlFireLocator = args[0];
	String sqlFireURL = "jdbc:sqlfire://" + sqlFireLocator;
	
//    SqlfDataSerializable.initTypes();

    JobConf conf = new JobConf(getConf());
    conf.setJobName("Busy Hotel Count");

    String hdfsHomeDir = "sta_tables";
    String tableName = "APP.BOOKING";
    Path intermediateOutputPath = new Path("/int_output");
    Path outputPath = new Path ("/output");
    intermediateOutputPath.getFileSystem(conf).delete(intermediateOutputPath, true);
    outputPath.getFileSystem(conf).delete(outputPath, true);

    conf.set(RowInputFormat.HOME_DIR, hdfsHomeDir);
    conf.set(RowInputFormat.INPUT_TABLE, tableName);
    conf.setBoolean(RowInputFormat.CHECKPOINT_MODE, false);
    
    conf.setInputFormat(RowInputFormat.class);
    conf.setMapperClass(HotelFinder.class);
    conf.setMapOutputKeyClass(LongWritable.class);
    conf.setMapOutputValueClass(IntWritable.class);
    
    conf.setReducerClass(HotelCounter.class);
    conf.setReducerClass(HotelCounter.class);
    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(IntWritable.class);
    
    FileOutputFormat.setOutputPath(conf, intermediateOutputPath);
    RunningJob job1 = JobClient.runJob(conf);
    int rc = job1.isSuccessful() ? 0 : 1;
    
    if (rc == 0) {
    	long numHotels = job1.getCounters().getCounter(Counter.REDUCE_OUTPUT_RECORDS);
    	System.out.println("Now running the HotelPrice updater");
    	
    	JobConf priceUpdateConf = new JobConf(getConf());
    	priceUpdateConf.setLong("NumHotels", numHotels);
    	priceUpdateConf.set("sqlf-connection-string", sqlFireURL);
    	priceUpdateConf.setJobName("Update Hotel Prices");
    	priceUpdateConf.setNumReduceTasks(1);
    	priceUpdateConf.setNumMapTasks(4);
    	
    	FileInputFormat.setInputPaths(priceUpdateConf, intermediateOutputPath);
    	priceUpdateConf.setInputFormat(TextInputFormat.class);
    	
    	//priceUpdateConf.set(RowOutputFormat.OUTPUT_URL, "jdbc:sqlfire://kuwait.gemstone.com:1527");
    	priceUpdateConf.set(RowOutputFormat.OUTPUT_URL, sqlFireURL);
    	priceUpdateConf.set(RowOutputFormat.OUTPUT_TABLE, "APP.HOTEL");
    	priceUpdateConf.setOutputFormat(RowOutputFormat.class);
    	
    	priceUpdateConf.setMapperClass(HotelCountInverter.class);
    	priceUpdateConf.setMapOutputKeyClass(IntWritable.class);
    	priceUpdateConf.setMapOutputValueClass(LongWritable.class);
    	
    	priceUpdateConf.setReducerClass(HotelPriceUpdater.class);
    	priceUpdateConf.setOutputKeyClass(Key.class);
    	priceUpdateConf.setOutputValueClass(BusyHotelModel.class);
    	rc = JobClient.runJob(priceUpdateConf).isSuccessful() ? 0 : 1;
    }
    
    return rc;
  }

  public static void main(String[] args) throws Exception {
    System.out.println("Hotel Updater invoked with  " + args);
    int rc = ToolRunner.run(new TopBusyHotel(), args);
    System.exit(rc);
  }
  
}
