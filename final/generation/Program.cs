using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using System.Data;
using System.Diagnostics;

namespace ClimateMySQL
{
    class Program
    {
        static int countMeasurements = 10;
        static List<long> measurements = new List<long>();

        static void dropTable(MySqlConnection conn)
        {
            String query = "drop table if exists test;";

            using (MySqlCommand sqlCommand = new MySqlCommand(query, conn))
            {
                sqlCommand.ExecuteNonQuery();
            }

        }

        static void createTable(MySqlConnection conn)
        {
            String query = "create table test(station_id INT,date DATE NOT NULL, hour SMALLINT NOT NULL, air_temperature SMALLINT NOT NULL DEFAULT -9999, dew_point SMALLINT NOT NULL DEFAULT - 9999, sea_level SMALLINT NOT NULL DEFAULT - 9999, wind_direction SMALLINT NOT NULL DEFAULT - 9999, wind_speed SMALLINT NOT NULL DEFAULT - 9999, cloud_cover SMALLINT NOT NULL DEFAULT - 9999, one_hour_alp SMALLINT NOT NULL DEFAULT - 9999, six_hour_alp SMALLINT NOT NULL DEFAULT - 9999, PRIMARY KEY (station_id, date, hour))";

            using (MySqlCommand sqlCommand = new MySqlCommand(query, conn))
            {
                sqlCommand.ExecuteNonQuery();
            }
        }

        // multiple insert with parameters in one query  
        static void generateData(MySqlConnection conn, int countStations)
        {
            int stationId = 10010;
            int hour = 0;
            Date date = new Date(2015, 1, 1);
            int countObservations = 365 * 24;
            Random random = new Random();
            int[] six_hour_alp = new int[2];
            six_hour_alp[1] = -9999;
            StringBuilder builder = new StringBuilder();

            builder.Append("insert into test(station_id, date, hour, air_temperature, dew_point, sea_level, wind_direction, wind_speed, cloud_cover, six_hour_alp) values ");

            for (int j = 0; j < countObservations; j++)
            {
                builder.AppendFormat("(@station_id{0}, @date{0}, @hour{0}, @air_temperature{0}, @dew_point{0}, @sea_level{0}, @wind_direction{0}, @wind_speed{0}, @cloud_cover{0}, @six_hour_alp{0}),", j);

            }

            builder.Replace(',', ';', builder.Length - 1, 1);

            using (MySqlCommand command = new MySqlCommand(builder.ToString(), conn))
            {
                command.Prepare();

                for (int i = 0; i < countStations; i++)
                {

                    for (int j = 0; j < countObservations; j++)
                    {
                        command.Parameters.AddWithValue("@station_id" + j, stationId);
                        command.Parameters.AddWithValue("@date" + j, date.ToString("s"));
                        command.Parameters.AddWithValue("@hour" + j, hour);
                        command.Parameters.AddWithValue("@air_temperature" + j, random.Next(-200, 201));
                        command.Parameters.AddWithValue("@dew_point" + j, random.Next(-200, 201));
                        command.Parameters.AddWithValue("@sea_level" + j, random.Next(9000, 11001));
                        command.Parameters.AddWithValue("@wind_direction" + j, random.Next(0, 361));
                        command.Parameters.AddWithValue("@wind_speed" + j, random.Next(0, 301));
                        command.Parameters.AddWithValue("@cloud_cover" + j, random.Next(0, 20));
                        command.Parameters.AddWithValue("@six_hour_alp" + j, six_hour_alp[random.Next(0, 2)]);

                        if (hour < 23)
                        {
                            hour += 1;
                        }
                        else
                        {
                            hour = 0;
                            date = date.AddDays(1);
                        }

                    }

                    command.ExecuteNonQuery();
                    command.Parameters.Clear();

                    stationId += 1;
                    date = new Date(2015, 1, 1);
                    hour = 0;

                }
            }

        }

        static void Main(string[] args)
        {

            string ServerName = "radagast.asuscomm.com";
            string UserName = "DBoptimisation";
            string DbName = "climatic_data";
            string Rassword = "password";
            string ConnStr = "server=" + ServerName +
                ";user=" + UserName +
                ";database=" + DbName +
                ";port=" + port +
                ";password=" + Rassword + ";";
            int countStations = 100;


            using (MySqlConnection conn = new MySqlConnection(ConnStr))
            {
                try
                {

                    conn.Open();

                    for (int m = 0; m < countMeasurements; m++)
                    {
                        Stopwatch sw = new Stopwatch();
                        dropTable(conn);
                        createTable(conn);
                        sw.Start();
                        generateData(conn, countStations);
                        sw.Stop();

                        long time = sw.ElapsedMilliseconds;
                        Console.WriteLine("Measurement " + (m + 1) + " time = " + time);
                        measurements.Add(time);
                    }


                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    Console.WriteLine(e.StackTrace);

                }

            }


            Console.WriteLine("End");
            Console.WriteLine("Average time = " + measurements.Average());
            Console.ReadKey();
        }
    }
}
