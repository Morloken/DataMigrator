//using System;
//using System.Collections.Generic;
//using System.Globalization;
//using System.IO;
//using Microsoft.Extensions.Configuration;
//using Npgsql;

//class Program
//{
//    static void Main()
//    {
//        // --- Налаштування конфігурації ---
//        var builder = new ConfigurationBuilder()
//            .SetBasePath(AppContext.BaseDirectory)
//            .AddJsonFile("C:\\Users\\user1\\Documents\\GitHub\\DataMigrator\\DataMigrator\\appsettings.json", optional: false, reloadOnChange: true);

//        var configuration = builder.Build();
//        string targetConnStr = configuration.GetConnectionString("AtmosphereDB");

//        Console.WriteLine("Підключення до БД: " + targetConnStr);

//        // --- CSV файл ---
//        string csvFile = "C:\\Users\\user1\\Documents\\Tables\\Category.csv";

//        // --- Мапінг стовпців CSV -> таблиця Atmosphere ---
//        var columnMapping = new Dictionary<string, string>
//        {
//            { "ID_Category", "id_category" },
//            { "Designation", "designation" }
//        };

//        MigrateCsvToDb(csvFile, "Category", columnMapping, targetConnStr);

//        Console.WriteLine("Міграція завершена потужно!");
//    }

//    static void MigrateCsvToDb(string csvPath, string targetTable, Dictionary<string, string> columnMapping, string targetConnStr)
//    {
//        if (!File.Exists(csvPath))
//        {
//            Console.WriteLine("CSV файл не знайдено: " + csvPath);
//            return;
//        }

//        var lines = File.ReadAllLines(csvPath);

//        if (lines.Length < 2)
//        {
//            Console.WriteLine("CSV порожній або тільки заголовок.");
//            return;
//        }

//        // Заголовки CSV
//        var headers = lines[0].Split(',');

//        using var conn = new NpgsqlConnection(targetConnStr);
//        conn.Open();

//        for (int i = 1; i < lines.Length; i++)
//        {
//            var values = lines[i].Split(',');

//            var columns = new List<string>();
//            var paramNames = new List<string>();
//            var parameters = new List<NpgsqlParameter>();

//            for (int j = 0; j < headers.Length; j++)
//            {
//                string csvCol = headers[j];
//                if (!columnMapping.ContainsKey(csvCol)) continue;

//                string dbCol = columnMapping[csvCol];

//                object rawValue = values[j];
//                object convertedValue = ConvertValue(rawValue);

//                columns.Add(dbCol);
//                string paramName = "@" + dbCol;
//                paramNames.Add(paramName);
//                parameters.Add(new NpgsqlParameter(paramName, convertedValue));
//            }

//            string insertQuery = $"INSERT INTO {targetTable} ({string.Join(",", columns)}) VALUES ({string.Join(",", paramNames)})";

//            using var cmd = new NpgsqlCommand(insertQuery, conn);
//            cmd.Parameters.AddRange(parameters.ToArray());
//            cmd.ExecuteNonQuery();
//        }

//        conn.Close();
//    }

//    static object ConvertValue(object value)
//    {
//        if (value == null || value.ToString() == "")
//            return DBNull.Value;

//        string s = value.ToString().Trim();

//        // int з CSV у CHAR(36)
//        if (int.TryParse(s, out int i))
//            return i.ToString();

//        // double
//        if (double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out double d))
//            return Math.Round(d, 2);

//        // DateTime
//        if (DateTime.TryParse(s, out DateTime dt))
//            return dt;

//        return s;
//    }
//}
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
        string csvFile = Path.Combine(AppContext.BaseDirectory, "C:\\Users\\user1\\Documents\\Tables\\Category.csv");

        // --- Мапінг стовпців CSV -> таблиця Atmosphere ---
        var columnMapping = new Dictionary<string, string>
        {
            { "ID_Category", "id_category" },
            { "Designation", "designation" }
        };

        MigrateCsvToDb(csvFile, "Category", columnMapping, targetConnStr);

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

        for (int i = 1; i < lines.Length; i++)
        {
            var values = lines[i].Split(',');

            var columns = new List<string>();
            var paramNames = new List<string>();
            var parameters = new List<NpgsqlParameter>();

            for (int j = 0; j < headers.Length; j++)
            {
                string csvCol = headers[j];
                if (!columnMapping.ContainsKey(csvCol)) continue;

                string dbCol = columnMapping[csvCol];
                object rawValue = values[j];
                object convertedValue = ConvertValue(rawValue, dbCol);

                columns.Add(dbCol);
                string paramName = "@" + dbCol;
                paramNames.Add(paramName);
                parameters.Add(new NpgsqlParameter(paramName, convertedValue));
            }

            string insertQuery = $"INSERT INTO {targetTable} ({string.Join(",", columns)}) VALUES ({string.Join(",", paramNames)})";

            using var cmd = new NpgsqlCommand(insertQuery, conn);
            cmd.Parameters.AddRange(parameters.ToArray());
            cmd.ExecuteNonQuery();
        }

        conn.Close();
    }

    static object ConvertValue(object value, string dbColumn)
    {
        if (value == null || string.IsNullOrWhiteSpace(value.ToString()))
            return DBNull.Value;

        string s = value.ToString().Trim();

        // 🔹 Якщо стовпець UUID, генеруємо UUID на основі числа з CSV
        if (dbColumn == "id_category" && int.TryParse(s, out int intId))
        {
            byte[] bytes = new byte[16];
            BitConverter.GetBytes(intId).CopyTo(bytes, 0);
            return new Guid(bytes);
        }

        // double
        if (double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out double d))
            return Math.Round(d, 2);

        // DateTime
        if (DateTime.TryParse(s, out DateTime dt))
            return dt;

        return s;
    }
}
