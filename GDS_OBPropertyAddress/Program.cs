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
                string connStr = "Provider=OraOLEDB.Oracle;Data Source=NEPSTRN;User Id=NEPSBI;Password=xs2nepsbi";
                OleDbConnection conn = new OleDbConnection(connStr);
                conn.Open();

                OleDbCommand cmd_Property = new OleDbCommand();
                cmd_Property.Connection = conn;
                cmd_Property.CommandText = "SELECT DP_ID, SERVICE_NUMBER, FLOOR, BUILDING_NAME, LOT_NUMBER, STREET_NAME, STREET_TYPE, SECTION_NAME,CITY, STATE_CODE, POSTAL_CODE, COUNTRY, ROWID FROM BI_PROPERTY_ADDRESS WHERE BI_BATCH_ID is null";
                cmd_Property.CommandType = CommandType.Text;
                OleDbDataReader dr_Property = cmd_Property.ExecuteReader();

                if (dr_Property.HasRows)
                {
                    List<string> lines = new List<string>();

                    //OleDbCommand cmd_GetBID = new OleDbCommand();
                    //cmd_GetBID.Connection = conn;
                    //cmd_GetBID.CommandText = "SELECT BI_BATCH_SEQ.NEXTVAL AS BID FROM DUAL";
                    //cmd_GetBID.CommandType = CommandType.Text;
                    //OleDbDataReader dr_BID = cmd_GetBID.ExecuteReader();

                    ////get batch id
                    //dr_BID.Read();
                    //string bid = dr_BID.GetDecimal(0).ToString();

                    //OleDbCommand cmd_SetStartTime = new OleDbCommand();
                    //cmd_SetStartTime.Connection = conn;
                    //cmd_SetStartTime.CommandText = "INSERT INTO BI_BATCH(BATCH_ID, INSTANCE_ID, CLASS_NAME, TIME_START, SERVICE_NAME, TYPE, FILE_HAS_ERROR) VALUES(:bid, 'GDS_DeletedNetworkTermination', 'EdgeFrontier.GDS.OBDeletedNetworkTermination', SysDate, 'GDS', 'OUTBOUND', 0)";
                    //cmd_SetStartTime.Parameters.AddWithValue(":bid", bid);
                    //cmd_SetStartTime.CommandType = CommandType.Text;
                    //cmd_SetStartTime.ExecuteNonQuery();
                    //cmd_SetStartTime.Dispose();

                    while (dr_Property.Read())
                    {
                        string DP_ID = dr_Property.GetString(0);
                        string SERVICE_NUMBER = dr_Property.GetDecimal(1).ToString();
                        string FLOOR = dr_Property.GetString(2);
                        string BUILDING_NAME = dr_Property.GetString(3);
                        string LOT_NUMBER = dr_Property.GetString(4);
                        string STREET_NAME = dr_Property.GetString(5);
                        string STREET_TYPE = dr_Property.GetString(6);
                        string SECTION_NAME = dr_Property.GetString(7);
                        string CITY = dr_Property.GetString(8);
                        string STATE_CODE = dr_Property.GetString(9);
                        string POSTAL_CODE = dr_Property.GetDecimal(10).ToString();
                        string COUNTRY = dr_Property.GetString(11);
                        string ROWID = dr_Property.GetString(12);

                        string line = String.Format("{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}|{8}|{9}|{10}|{11}|", DP_ID, SERVICE_NUMBER, FLOOR, BUILDING_NAME, LOT_NUMBER, STREET_NAME, STREET_TYPE, SECTION_NAME, CITY, STATE_CODE, POSTAL_CODE, COUNTRY);
                        lines.Add(line);

                        //OleDbCommand cmd_GetBIO = new OleDbCommand();
                        //cmd_GetBIO.Connection = conn;
                        //cmd_GetBIO.CommandText = "SELECT BI_INSERT_SEQ.NEXTVAL FROM BI_DEL_NET_TMNT WHERE ROWNUM = 1";
                        //cmd_GetBIO.CommandType = CommandType.Text;
                        //OleDbDataReader dr_BIO = cmd_GetBIO.ExecuteReader();

                        //dr_BIO.Read();
                        //string bio = dr_BIO.GetDecimal(0).ToString();

                        //cmd_GetBIO.Dispose();

                        //OleDbCommand cmd_UpdateProperty = new OleDbCommand();
                        //cmd_UpdateProperty.Connection = conn;
                        //cmd_UpdateProperty.CommandText = "UPDATE BI_DEL_NET_TMNT set BI_BATCH_ID = :bid_val, BI_INSERT_ORDER = :bio_val where rowid = :rowid_val";
                        //cmd_UpdateProperty.Parameters.AddWithValue(":bid_val", bid);
                        //cmd_UpdateProperty.Parameters.AddWithValue(":bio_val", bio);
                        //cmd_UpdateProperty.Parameters.AddWithValue(":rowid_val", ROWID);
                        //cmd_UpdateProperty.ExecuteNonQuery();
                        //cmd_UpdateProperty.Dispose();
                    }

                    string date = DateTime.Now.ToString("yyyyMMdd_HHmm");
                    string filename = "NIS_NEPS_ADDE_" + date + ".csv";

                    if (File.Exists(filename))
                    {
                        File.Delete(filename);
                    }

                    File.AppendAllLines(filename, lines);

                    //OleDbCommand cmd_SetEndTime = new OleDbCommand();
                    //cmd_SetEndTime.Connection = conn;
                    //cmd_SetEndTime.CommandText = "UPDATE BI_BATCH SET TIME_END = SysDate, FILENAME = :filename WHERE BATCH_ID = :bid";
                    //cmd_SetEndTime.Parameters.AddWithValue(":filename", filename);
                    //cmd_SetEndTime.Parameters.AddWithValue(":bid", bid);
                    //cmd_SetEndTime.CommandType = CommandType.Text;
                    //cmd_SetEndTime.ExecuteNonQuery();
                    //cmd_SetEndTime.Dispose();
                }

                dr_Property.Close();
                conn.Dispose();
                conn.Close();
            }
            else
            {
                Console.WriteLine("Please enter connection string.\nExample: \"Provider = OraOLEDB.Oracle; Data Source = NEPSTRN; User Id = NEPSBI; Password = xs2nepsbi\"");
            }
        }
    }
}
