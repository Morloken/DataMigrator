
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Microsoft.Extensions.Configuration;
using Npgsql;

class Program
{
    static void Main()
    {
        // --- Налаштування конфігурації ---
        var builder = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("C:\\Users\\user1\\Documents\\GitHub\\DataMigrator\\DataMigrator\\appsettings.json", optional: false, reloadOnChange: true);

        var configuration = builder.Build();
        string targetConnStr = configuration.GetConnectionString("AtmosphereDB");

        Console.WriteLine("Підключення до БД: " + targetConnStr);

        // --- CSV файл ---
        string csvFile = Path.Combine(AppContext.BaseDirectory, "C:\\Users\\user1\\Documents\\Tables\\Favouriote_Station.csv");

        // --- Мапінг стовпців CSV -> таблиця Atmosphere ---
        //var columnMapping = new Dictionary<string, string>
        //{
        //    { "ID_Category", "id_category" },
        //    { "Designation", "designation" }
        //};
        var columnMapping = new Dictionary<string, string>
    {//--------------------------------------------CHANGE
        { "ID_Station", "id_station" },  // з CSV в UUID
        { "Longitude", "location" },     // Longitude+Latitude в POINT
        { "Latitude", "location" }
    };

        MigrateCsvToDb(csvFile, "Station", columnMapping, targetConnStr);

        Console.WriteLine("Міграція завершена потужно!");
    }

    static void MigrateCsvToDb(string csvPath, string targetTable, Dictionary<string, string> columnMapping, string targetConnStr)
    {
        if (!File.Exists(csvPath))
        {
            Console.WriteLine("CSV файл не знайдено: " + csvPath);
            return;
        }

        var lines = File.ReadAllLines(csvPath);

        if (lines.Length < 2)
        {
            Console.WriteLine("CSV порожній або тільки заголовок.");
            return;
        }

        var headers = lines[0].Split(',');

        using var conn = new NpgsqlConnection(targetConnStr);
        conn.Open();


        //for (int i = 1; i < lines.Length; i++)
        //{
        //    var values = lines[i].Split(',');

        //    var columns = new List<string>();
        //    var paramNames = new List<string>();
        //    var parameters = new List<NpgsqlParameter>();

        //    for (int j = 0; j < headers.Length; j++)
        //    {
        //        string csvCol = headers[j];
        //        if (!columnMapping.ContainsKey(csvCol)) continue;

        //        string dbCol = columnMapping[csvCol];
        //        object rawValue = values[j];
        //        object convertedValue = ConvertValue(rawValue, dbCol);

        //        columns.Add(dbCol);
        //        string paramName = "@" + dbCol;
        //        paramNames.Add(paramName);
        //        parameters.Add(new NpgsqlParameter(paramName, convertedValue));
        //    }

        //    string insertQuery = $"INSERT INTO {targetTable} ({string.Join(",", columns)}) VALUES ({string.Join(",", paramNames)})";

        //    using var cmd = new NpgsqlCommand(insertQuery, conn);
        //    cmd.Parameters.AddRange(parameters.ToArray());
        //    cmd.ExecuteNonQuery();
        //}
        for (int i = 1; i < lines.Length; i++)
        {
            var values = lines[i].Split(',');
            string lon = null;
            string lat = null;
            object idStation = null;

            for (int j = 0; j < headers.Length; j++)
            {
                string csvCol = headers[j];
                string val = values[j].Trim();

                if (csvCol == "ID_Station")
                    idStation = Guid.NewGuid(); // або детермінований UUID
                else if (csvCol == "Longitude")
                    lon = val;
                else if (csvCol == "Latitude")
                    lat = val;
            }

            //            string insertQuery = @"
            //INSERT INTO Station (id_station, city, name, status, location) 
            //VALUES (@id, @city, @name, @status, @loc)";
            //            using var cmd = new NpgsqlCommand(insertQuery, conn);

            //            cmd.Parameters.AddWithValue("id", idStation);
            //            cmd.Parameters.AddWithValue("city", "Unknown");          // дефолтне місто
            //            cmd.Parameters.AddWithValue("name", "Station " + i);     // дефолтне ім'я
            //            cmd.Parameters.AddWithValue("status", false);            // дефолтний статус
            //            cmd.Parameters.AddWithValue("loc", new NpgsqlTypes.NpgsqlPoint(
            //                double.Parse(lon, CultureInfo.InvariantCulture),
            //                double.Parse(lat, CultureInfo.InvariantCulture)));

            //            cmd.ExecuteNonQuery();
            // вставка в Favourite_Station
            var insertCmd = new NpgsqlCommand(
                "INSERT INTO Favourite_Station (user_name, id_station) VALUES (@user, @station)", conn);

            insertCmd.Parameters.AddWithValue("user", rec.ID_User.ToString());
            insertCmd.Parameters.AddWithValue("station", (Guid)stationUuid);

            insertCmd.ExecuteNonQuery();
            Console.WriteLine($"✅ Додано: User {rec.ID_User} -> Station {rec.ID_Station}");


        }

        conn.Close();
    }

    //static object ConvertValue(object value, string dbColumn)
    //{
    //    if (value == null || string.IsNullOrWhiteSpace(value.ToString()))
    //        return DBNull.Value;

    //    string s = value.ToString().Trim();

    //    // 🔹 Якщо стовпець UUID, генеруємо UUID на основі числа з CSV
    //    if (dbColumn == "id_category" && int.TryParse(s, out int intId))
    //    {
    //        byte[] bytes = new byte[16];
    //        BitConverter.GetBytes(intId).CopyTo(bytes, 0);
    //        return new Guid(bytes);
    //    }

    //    // double
    //    if (double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out double d))
    //        return Math.Round(d, 2);

    //    // DateTime
    //    if (DateTime.TryParse(s, out DateTime dt))
    //        return dt;

    //    return s;
    //}
    static object ConvertValue(object value, string dbColumn, string latitude = null)
    {
        if (value == null || string.IsNullOrWhiteSpace(value.ToString()))
            return DBNull.Value;

        string s = value.ToString().Trim();

        if (dbColumn == "id_station")
        {
            // Конвертуємо рядок/число в UUID
            return Guid.NewGuid(); // або генерація на основі CSV ID
        }

        if (dbColumn == "location" && latitude != null)
        {
            // Створюємо POINT(x,y)
            return $"({s},{latitude})";
        }

        return s;
    }
}
