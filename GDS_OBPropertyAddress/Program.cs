using System;
using System.IO;
using System.Data;
using System.Data.OleDb;
using System.Collections.Generic;

namespace GDS_OBPropertyAddress
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length > 0)
            {
                #region 1. Get & Open Connection

                //"Provider=OraOLEDB.Oracle;Data Source=NEPSTRN;User Id=NEPSBI;Password=xs2nepsbi"
                string connStr = args[0];
                OleDbConnection conn = new OleDbConnection(connStr);
                conn.Open();

                #endregion

                #region 2. Process

                #region 2.1. Referencing data from table

                OleDbCommand cmd_Property = new OleDbCommand();
                cmd_Property.Connection = conn;
                cmd_Property.CommandText = "SELECT DP_ID, SERVICE_NUMBER, FLOOR, BUILDING_NAME, LOT_NUMBER, STREET_NAME, STREET_TYPE, SECTION_NAME, CITY, STATE_CODE, POSTAL_CODE, COUNTRY, ROWID FROM BI_PROPERTY_ADDRESS WHERE BI_BATCH_ID is null";
                cmd_Property.CommandType = CommandType.Text;
                OleDbDataReader dr_Property = cmd_Property.ExecuteReader();

                #endregion

                #region 2.2. Starts processing if there are values in the table

                if (dr_Property.HasRows)
                {
                    List<string> lines = new List<string>();

                    #region 2.2.1. Get Batch ID

                    OleDbCommand cmd_GetBID = new OleDbCommand();
                    cmd_GetBID.Connection = conn;
                    cmd_GetBID.CommandText = "SELECT BI_BATCH_SEQ.NEXTVAL AS BID FROM DUAL";
                    cmd_GetBID.CommandType = CommandType.Text;
                    OleDbDataReader dr_BID = cmd_GetBID.ExecuteReader();
                    
                    dr_BID.Read();
                    string bid = dr_BID.GetDecimal(0).ToString();
                    dr_BID.Close();
                    cmd_GetBID.Dispose();

                    #endregion

                    #region 2.2.2. Record StartTime

                    OleDbCommand cmd_SetStartTime = new OleDbCommand();
                    cmd_SetStartTime.Connection = conn;
                    cmd_SetStartTime.CommandText = "INSERT INTO BI_BATCH(BATCH_ID, INSTANCE_ID, CLASS_NAME, TIME_START, SERVICE_NAME, TYPE, FILE_HAS_ERROR) VALUES(:bid, 'GDS_PropertyAddress', 'EdgeFrontier.GDS.PropertyAddress', SysDate, 'GDS', 'OUTBOUND', 0)";
                    cmd_SetStartTime.Parameters.AddWithValue(":bid", bid);
                    cmd_SetStartTime.CommandType = CommandType.Text;
                    cmd_SetStartTime.ExecuteNonQuery();
                    cmd_SetStartTime.Dispose();

                    #endregion

                    #region 2.2.3. Starts reading and processing in details

                    while (dr_Property.Read())
                    {
                        //Store data from executed query into variables
                        string DP_ID = (!dr_Property.IsDBNull(0)) ? dr_Property.GetString(0) : "";
                        string SERVICE_NUMBER = (!dr_Property.IsDBNull(1)) ? dr_Property.GetDecimal(1).ToString() : "";
                        string FLOOR = (!dr_Property.IsDBNull(2)) ? dr_Property.GetString(2) : "";
                        string BUILDING_NAME = (!dr_Property.IsDBNull(3)) ? dr_Property.GetString(3) : "";
                        string LOT_NUMBER = (!dr_Property.IsDBNull(4)) ? dr_Property.GetString(4) : "";
                        string STREET_NAME = (!dr_Property.IsDBNull(5)) ? dr_Property.GetString(5) : "";
                        string STREET_TYPE = (!dr_Property.IsDBNull(6)) ? dr_Property.GetString(6) : "";
                        string SECTION_NAME = (!dr_Property.IsDBNull(7)) ? dr_Property.GetString(7) : "";
                        string CITY = (!dr_Property.IsDBNull(8)) ? dr_Property.GetString(8) : "";
                        string STATE_CODE = (!dr_Property.IsDBNull(9)) ? dr_Property.GetString(9) : "";
                        string POSTAL_CODE = (!dr_Property.IsDBNull(10)) ? dr_Property.GetDecimal(10).ToString() : "";
                        string COUNTRY = (!dr_Property.IsDBNull(11)) ? dr_Property.GetString(11) : "";
                        string ROWID = (!dr_Property.IsDBNull(12)) ? dr_Property.GetString(12) : "";

                        //Format line
                        string line = String.Format("{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}|{8}|{9}|{10}|{11}|", DP_ID, SERVICE_NUMBER, FLOOR, BUILDING_NAME, LOT_NUMBER, STREET_NAME, STREET_TYPE, SECTION_NAME, CITY, STATE_CODE, POSTAL_CODE, COUNTRY);

                        //Add line to list of output lines
                        lines.Add(line);

                        #region 2.2.3.1 Update table

                        OleDbCommand cmd_GetBIO = new OleDbCommand();
                        cmd_GetBIO.Connection = conn;
                        cmd_GetBIO.CommandText = "SELECT BI_INSERT_SEQ.NEXTVAL FROM BI_PROPERTY_ADDRESS WHERE ROWNUM = 1";
                        cmd_GetBIO.CommandType = CommandType.Text;
                        OleDbDataReader dr_BIO = cmd_GetBIO.ExecuteReader();

                        dr_BIO.Read();
                        string bio = dr_BIO.GetDecimal(0).ToString();
                        dr_BIO.Close();
                        cmd_GetBIO.Dispose();

                        OleDbCommand cmd_UpdateProperty = new OleDbCommand();
                        cmd_UpdateProperty.Connection = conn;
                        cmd_UpdateProperty.CommandText = "UPDATE BI_PROPERTY_ADDRESS set BI_BATCH_ID = :bid_val, BI_INSERT_ORDER = :bio_val where rowid = :rowid_val";
                        cmd_UpdateProperty.Parameters.AddWithValue(":bid_val", bid);
                        cmd_UpdateProperty.Parameters.AddWithValue(":bio_val", bio);
                        cmd_UpdateProperty.Parameters.AddWithValue(":rowid_val", ROWID);
                        cmd_UpdateProperty.ExecuteNonQuery();
                        cmd_UpdateProperty.Dispose();

                        #endregion
                    }

                    #endregion

                    #region 2.2.4. Write CSV file

                    string date = DateTime.Now.ToString("yyyyMMdd_HHmm");
                    string filename = "NIS_NEPS_ADDE_" + date + ".csv";

                    if (File.Exists(filename))
                    {
                        File.Delete(filename);
                    }

                    //Write data to file
                    File.AppendAllLines(filename, lines);

                    #endregion
                    
                    #region 2.2.5. Record EndTime

                    OleDbCommand cmd_SetEndTime = new OleDbCommand();
                    cmd_SetEndTime.Connection = conn;
                    cmd_SetEndTime.CommandText = "UPDATE BI_BATCH SET TIME_END = SysDate, FILENAME = :filename WHERE BATCH_ID = :bid";
                    cmd_SetEndTime.Parameters.AddWithValue(":filename", filename);
                    cmd_SetEndTime.Parameters.AddWithValue(":bid", bid);
                    cmd_SetEndTime.CommandType = CommandType.Text;
                    cmd_SetEndTime.ExecuteNonQuery();
                    cmd_SetEndTime.Dispose();

                    #endregion
                }

                #endregion

                #endregion

                #region 3. Close Connection

                dr_Property.Close();
                cmd_Property.Dispose();
                conn.Dispose();
                conn.Close();

                #endregion
            }
            else
            {
                Console.WriteLine("Please enter connection string.\nExample: \"Provider = OraOLEDB.Oracle; Data Source = NEPSTRN; User Id = NEPSBI; Password = xs2nepsbi\"");
            }
        }
    }
}
