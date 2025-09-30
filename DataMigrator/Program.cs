using System;
using System.Globalization;
using System.IO;
using Microsoft.Extensions.Configuration;
using Npgsql;

class Program
{
    static void Main()
    {
        var builder = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("C:\\Users\\user1\\Documents\\GitHub\\DataMigrator\\DataMigrator\\appsettings.json", optional: false, reloadOnChange: true);

        var configuration = builder.Build();
        string targetConnStr = configuration.GetConnectionString("AtmosphereDB");

        Console.WriteLine("Підключення до БД: " + targetConnStr);

        string csvFile = Path.Combine(AppContext.BaseDirectory, "C:\\Users\\user1\\Documents\\Tables\\Station.csv");

        InsertStations(csvFile, targetConnStr);

        Console.WriteLine("Міграція Station завершена потужно!");
    }

    static void InsertStations(string csvPath, string connStr)
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

        using var conn = new NpgsqlConnection(connStr);
        conn.Open();

        for (int i = 1; i < lines.Length; i++)
        {
            var values = lines[i].Split(',');
            if (values.Length < 6)
            {
                Console.WriteLine($"Пропускаємо рядок {i} – недостатньо колонок.");
                continue;
            }

            string cityCsv = values[0].Trim();
            string nameCsv = values[1].Trim().Trim('"');
            string idStationCsv = values[2].Trim();
            string statusCsv = values[3].Trim();
            string idServerStr = values[4].Trim();
            string idSaveEcoBotStr = values[5].Trim();

            bool status = statusCsv.Equals("enabled", StringComparison.OrdinalIgnoreCase);

            // Перевіряємо, чи станція вже є
            Guid idStation;
            using (var cmdCheck = new NpgsqlCommand("SELECT id_station FROM station WHERE id_station::text LIKE @csvId LIMIT 1", conn))
            {
                cmdCheck.Parameters.AddWithValue("csvId", "%" + idStationCsv);
                var result = cmdCheck.ExecuteScalar();
                idStation = result != null ? (Guid)result : Guid.NewGuid();
            }

            // UUID сервера (може бути NULL)
            Guid? idServer = null;
            if (!string.IsNullOrEmpty(idServerStr) && idServerStr != "NULL")
            {
                using var cmdServer = new NpgsqlCommand("SELECT id_server FROM mqtt_server WHERE id_server::text LIKE @idCsv LIMIT 1", conn);
                cmdServer.Parameters.AddWithValue("idCsv", idServerStr + "%");
                var result = cmdServer.ExecuteScalar();
                if (result != null)
                    idServer = (Guid)result;
            }

            
            Guid? idSaveEcoBot = null;

            // location - безпечна вставка point
            double x = 0, y = 0; // якщо координати відсутні, можна поставити 0,0
            string insertQuery = @"
                INSERT INTO station (id_station, city, name, status, id_server, id_saveecobot, location)
                VALUES (@idStation, @city, @name, @status, @idServer, @idSaveEcoBot, point(@x, @y))
                ON CONFLICT (id_station) DO UPDATE
                SET city = EXCLUDED.city,
                    name = EXCLUDED.name,
                    status = EXCLUDED.status,
                    id_server = EXCLUDED.id_server,
                    id_saveecobot = EXCLUDED.id_saveecobot
            ";

            using var insertCmd = new NpgsqlCommand(insertQuery, conn);
            insertCmd.Parameters.AddWithValue("idStation", idStation);
            insertCmd.Parameters.AddWithValue("city", cityCsv);
            insertCmd.Parameters.AddWithValue("name", nameCsv);
            insertCmd.Parameters.AddWithValue("status", status);
            insertCmd.Parameters.AddWithValue("idServer", (object)idServer ?? DBNull.Value);
            insertCmd.Parameters.AddWithValue("idSaveEcoBot", DBNull.Value);
            insertCmd.Parameters.AddWithValue("x", x);
            insertCmd.Parameters.AddWithValue("y", y);

            insertCmd.ExecuteNonQuery();

            Console.WriteLine($"Оброблено рядок {i}: {nameCsv} -> UUID {idStation}");
        }

        conn.Close();
    }
}
