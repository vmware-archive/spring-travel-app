package com.gopivotal.sta.jdbc;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/*****
 * This application queries the HDFS data using HAWQ and updates the in-memory data in GemfireXD based on that.
 * The HAWQ query fetches list of hotel_id sorted in descending order by the number of bookings.
 * Based on the query , we increase the prices of hotels in the top-half of the list and decrease the 
 * @author bansods
 *
 */
public class HotelPriceUpdater {
	
	public static final String HAWQ_URL = "hawq-url";
	public static final String HAWQ_USERNAME = "hawq-username";
	public static final String HAWQ_PASSWORD = "hawq-password";
	public static final String GFXD_URL = "gfxd-url";
	public static final String GFXD_USERNAME = "gfxd-username";
	public static final String GFXD_PASSWORD = "gfxd-password";
	
	private static final String HAWQ_DRIVER = "org.postgresql.Driver";

	private String hawqUrl;
	private String hawqUserName;
	private String hawqPassword;


	private String gfxdUrl;
	private String gfxdUserName;
	private String gfxdPassword;
	
	private HotelPriceUpdater (HotelPriceUpdaterBuilder builder) {
		this.hawqUrl = builder.hawqURL;
		this.hawqUserName = builder.hawqUserName;
		this.hawqPassword = builder.hawqPassword;
		this.gfxdUrl = builder.gemfireXdURL;
		this.gfxdUserName = builder.gemfireXdUsername;
		this.gfxdPassword  = builder.gemfireXdPassword;
	}
	
	
	private  List<Long>  getTopHotelsFromHawq() {
		List <Long> hotelIdList = new LinkedList<Long>();
		Connection hawqConnection = null;
		final String selectQuery = "select hotel_id  from Booking GROUP by hotel_id ORDER BY count(hotel_id) DESC";
		try {
			Properties props = new Properties();
			props.setProperty("user", "bansods");
			props.setProperty("password", "");
			Class.forName(HAWQ_DRIVER);
			System.out.println(hawqUrl);
			hawqConnection = DriverManager.getConnection(hawqUrl, props);

			Statement stmt = hawqConnection.createStatement();
			
			System.out.println("Querying the 'booking' table on HDFS using HAWQ...");
			System.out.println("Getting the list of hotels ordered by the number of bookings in descending order.");
			ResultSet rs = stmt.executeQuery(selectQuery);
			
			while(rs.next()) {
				long hotel_id = rs.getLong("hotel_id");
				hotelIdList.add(hotel_id);
			}
			
			if (!hotelIdList.isEmpty()) {
				System.out.println("\nHotel_ID");
				for (Long hotelId : hotelIdList) {
					System.out.println(hotelId.toString());
				}
			}else {
				System.out.println("No bookings found");
			}
			rs.close();
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			try {
				if (hawqConnection != null)
					hawqConnection.close();
			} catch (SQLException e) {
			}
		}
		return hotelIdList;
	}
	
	/*****
	 * Updates the prices of hotel according to the number of bookings, increases the price of the top half of the list by 
	 * 10% and decreases the prices of the bottom half by 10%  
	 * @param hotelIdList List of hotel_id sorted by number of bookings in descending order.
	 */
	private void updateHotelPricesInGfxd(List<Long> hotelIdList) {
		Connection gfxdConnection = null;
		PreparedStatement prepStatement = null;
		final String updateQuery = "UPDATE HOTEL SET PRICE=PRICE*? WHERE ID = ?"; 
		final double factor = 10;
		
		if (!hotelIdList.isEmpty()) {
			System.out.println("\nUpdating the prices of hotels in 'hotel' table in GemfireXD ....");
			try {
				gfxdConnection = DriverManager.getConnection("jdbc:sqlfire://kuwait.gemstone.com:1527/");
				prepStatement = gfxdConnection.prepareStatement(updateQuery);

				int numHotels = hotelIdList.size();
				int counter = 0;
				double multiplier;
				
				for (Long hotelId : hotelIdList) {
					counter++;
					if (counter <= numHotels/2) {
						multiplier = 1 + factor/100.0;
					}else {
						multiplier = 1 - factor/100.0;
					}
					prepStatement.setDouble(1, multiplier);
					prepStatement.setLong(2, hotelId);
					prepStatement.executeUpdate();
					System.out.println("Updated the price of Hotel with HotelID : " + hotelId + " by   " + multiplier + "x");
				}
				if (prepStatement != null) {
					prepStatement.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (gfxdConnection != null) {
					try {
						gfxdConnection.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	public void updateHotelPrices() {
		updateHotelPricesInGfxd(getTopHotelsFromHawq());
	}
	
	public static class HotelPriceUpdaterBuilder {
		private String hawqURL;
		private String hawqUserName;
		private String hawqPassword;
		private String gemfireXdURL;
		private String gemfireXdUsername;
		private String gemfireXdPassword;
		
		public HotelPriceUpdaterBuilder() {
			
		}
		public HotelPriceUpdaterBuilder hawqURL (String hawqURL) {
			this.hawqURL = hawqURL.trim();
			return this;
		}
		
		public HotelPriceUpdaterBuilder hawqUserName (String hawqUserName) {
			this.hawqUserName = hawqUserName;
			return this;
		}
		
		public HotelPriceUpdaterBuilder hawqPassword (String hawqPassword) {
			this.hawqPassword = hawqPassword;
			return this;
		}
		
		public HotelPriceUpdaterBuilder gemfireXdURL(String gemfireXdURL) {
			this.gemfireXdURL = gemfireXdURL;
			return this;
		}
		
		public HotelPriceUpdaterBuilder gemfireXdUsername (String gemfireXdUsername) {
			this.gemfireXdUsername = gemfireXdUsername;
			return this;
		}
		
		public HotelPriceUpdaterBuilder gemfireXdPassword (String gemfireXdPassword) {
			this.gemfireXdPassword = gemfireXdPassword;
			return this;
		}
		
		public HotelPriceUpdater build() {
			if (this.hawqURL == null || this.hawqURL.isEmpty()) {
				throw new IllegalArgumentException("Please specify the " + HAWQ_URL + " = jdbc:postgresql://hostname:port/database");
			}
			if (this.hawqUserName == null || this.hawqUserName.isEmpty()) {
				this.hawqUserName = "";
			}
			if (this.hawqPassword == null || this.hawqPassword.isEmpty()) {
				this.hawqPassword = "";
			}
			if (this.gemfireXdURL == null || this.gemfireXdURL.isEmpty()) {
				throw new IllegalArgumentException("Please specify the " + GFXD_URL + " = jdbc:sqlfire://locator_hostname:port");
			}
			if (this.gemfireXdPassword == null) {
				this.gemfireXdPassword = "";
			}
			if (this.gemfireXdUsername == null) {
				this.gemfireXdUsername = "";
			}
			
			return new HotelPriceUpdater(this);
		}
	}

	
	public static void main(String [] args) {
		try {
			
			if (args.length == 0) {
				System.out.println("Please provide the properties file with HAWQ and GFXD information, refer to the example jdbc.properties file in resources");
				return;
			}
			
			String filePath = args[0];
			Properties prop = new Properties();
			
			prop.load(new FileInputStream(filePath));
			
			
			String hawqURL = prop.getProperty(HotelPriceUpdater.HAWQ_URL);
			String hawqUserName = prop.getProperty(HotelPriceUpdater.HAWQ_USERNAME);
			String hawqPassword = prop.getProperty(HotelPriceUpdater.HAWQ_PASSWORD);
			String gemfireXdURL=  prop.getProperty(HotelPriceUpdater.GFXD_URL);
			String gemfireXdUsername = prop.getProperty(HotelPriceUpdater.GFXD_USERNAME);
			String gemfireXdPassword = prop.getProperty(HotelPriceUpdater.GFXD_PASSWORD);
			
			System.out.println(HotelPriceUpdater.HAWQ_URL + " : " + hawqURL);
			System.out.println(HotelPriceUpdater.GFXD_URL + " : " + gemfireXdURL);
			
			HotelPriceUpdater hotelPriceUpdater = new HotelPriceUpdater.HotelPriceUpdaterBuilder().hawqURL(hawqURL)
												  .hawqUserName(hawqUserName)
												  .hawqPassword(hawqPassword)
												  .gemfireXdURL(gemfireXdURL)
												  .gemfireXdUsername(gemfireXdUsername)
												  .gemfireXdPassword(gemfireXdPassword)
												  .build();
			
			hotelPriceUpdater.updateHotelPrices();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
