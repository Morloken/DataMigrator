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
        var builder = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("C:\\Users\\user1\\Documents\\GitHub\\DataMigrator\\DataMigrator\\appsettings.json", optional: false, reloadOnChange: true);

        var configuration = builder.Build();
        string targetConnStr = configuration.GetConnectionString("AtmosphereDB");

        Console.WriteLine("Підключення до БД: " + targetConnStr);

        string csvFile = Path.Combine(AppContext.BaseDirectory, "C:\\Users\\user1\\Documents\\Tables\\Favorite_Station.csv");

        InsertFavouriteStations(csvFile, targetConnStr);

        Console.WriteLine("Міграція Favorite_Station завершена потужно!");
    }

    static void InsertFavouriteStations(string csvPath, string connStr)
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

        // 🔹 Словник відповідності старих ID → UUID з Station
        var oldIdToUuid = new Dictionary<string, Guid>
        {
            { "0002", Guid.Parse("204b89c5-a3a2-4254-876e-e95b139f06eb") },
            { "0003", Guid.Parse("21390337-e4c2-4081-86fe-5515a01a6aea") },
            { "0004", Guid.Parse("2f91a5d9-ea67-4f4a-9d2b-a0ed21c43267") },
            { "0014", Guid.Parse("e67e9c35-9e96-4c6a-af6c-11e0e5633140") }
            // 🔹 додай інші, якщо будуть
        };

        using var conn = new NpgsqlConnection(connStr);
        conn.Open();

        for (int i = 1; i < lines.Length; i++)
        {
            var values = lines[i].Split(',');
            string idUser = values[0].Trim();
            string oldStationId = values[1].Trim();

            if (!oldIdToUuid.ContainsKey(oldStationId))
            {
                Console.WriteLine($"Пропускаємо: Station {oldStationId} немає у словнику відповідності.");
                continue;
            }

            Guid stationUuid = oldIdToUuid[oldStationId];

            // Вставка у Favourite_Station
            string insertQuery = "INSERT INTO Favorite_Station (user_name, id_station) VALUES (@user, @station)";
            using var insertCmd = new NpgsqlCommand(insertQuery, conn);
            insertCmd.Parameters.AddWithValue("user", idUser);
            insertCmd.Parameters.AddWithValue("station", stationUuid);
            insertCmd.ExecuteNonQuery();

            Console.WriteLine($"Додано: User {idUser} -> Station {oldStationId}");
        }

        conn.Close();
    }
}
