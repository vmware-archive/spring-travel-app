package com.gopivotal.mr;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * Model of the BUSY_HOTEL table:
 *
 * CREATE TABLE BUSY_HOTEL
 *    (
 *        HOTELID BIGINT,
 *        COUNT INT
 *    )
 *
 * This class is the glue that bridges the output of a reducer to an actual
 * SqlFire table.
 */
public class BusyHotelModel {

  private Long hotelId;
  private int count;
  //long timestamp;

  public BusyHotelModel(Long hotelId, int count) {
    this.hotelId = hotelId;
    this.count = count;
  //  this.timestamp = System.currentTimeMillis();
  }

  public void setHotelId(int idx, PreparedStatement ps) throws SQLException {
    ps.setLong(idx, hotelId);
  }

  public void setCount(int idx, PreparedStatement ps) throws SQLException {
    ps.setInt(idx, count);
  }

//  public void setStamp(int idx, PreparedStatement ps) throws SQLException {
//    ps.setTimestamp(idx, new Timestamp(timestamp));
//  }
}
