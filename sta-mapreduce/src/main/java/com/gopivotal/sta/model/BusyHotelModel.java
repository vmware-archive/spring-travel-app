package com.gopivotal.sta.model;


import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * Model of the Hotel table:
 *
 * Updates the following fields 
 * ID BIGINT
 * PRICE DOUBLE
 *
 * This class is the glue that bridges the output of a reducer to an actual
 * SqlFire table.
 */
public class BusyHotelModel {

	private Long hotelId;
	private String address; 
	private double price;
	private String city; 
	private String country;
	private String name;
	private String state; 
	private String zip;


	public BusyHotelModel(Long hotelId, String address, double price, String city, String country, String name, String state, String zip) {
		this.hotelId = hotelId;
		this.price = price;
		this.address = address; 
		this.city = city;
		this.country = country;
		this.name = name;
		this.state = state;
		this.zip = zip;
	}

	public void setId(int idx, PreparedStatement ps) throws SQLException {
		ps.setLong(idx, hotelId);
	}

	public void setPrice (int idx, PreparedStatement ps) throws SQLException {
		ps.setDouble(idx, price);
	}
	
	public double getPrice() {
		return this.price;
	}
	
	public void setNewPrice(double price) {
		this.price = price;
	}

	public void setAddress (int idx, PreparedStatement ps) throws SQLException {
		ps.setString(idx, address);
	}
	
	public String getName() {
		return this.name;
	}
	public void setCity (int idx, PreparedStatement ps) throws SQLException {
		ps.setString(idx, city);
	}

	public void setName (int idx, PreparedStatement ps) throws SQLException {
		ps.setString(idx, name);
	}

	public void setState (int idx, PreparedStatement ps) throws SQLException {
		ps.setString(idx, state);
	}

	public void setCountry (int idx, PreparedStatement ps) throws SQLException {
		ps.setString(idx, country);
	}
	public void setZip (int idx, PreparedStatement ps) throws SQLException {
		ps.setString(idx, zip);
	}
}
