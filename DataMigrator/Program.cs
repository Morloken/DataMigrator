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

        string csvFile = Path.Combine(AppContext.BaseDirectory, "C:\\Users\\user1\\Documents\\Tables\\Measured_Unit.csv");

        InsertMeasuredUnits(csvFile, targetConnStr);

        Console.WriteLine("Міграція Measured_Unit завершена потужно!");
    }

    static void InsertMeasuredUnits(string csvPath, string connStr)
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
            if (values.Length < 3)
            {
                Console.WriteLine($"Пропускаємо рядок {i} – недостатньо колонок.");
                continue;
            }

            string title = values[0].Trim();
            string unit = values[1].Trim();
            // старий ID можна використати для логів або відстеження, але для БД генеруємо UUID
            string oldId = values[2].Trim();
            Guid newId = Guid.NewGuid();

            string insertQuery = "INSERT INTO measured_unit (id_measured_unit, title, unit) VALUES (@id, @title, @unit)";
            using var insertCmd = new NpgsqlCommand(insertQuery, conn);
            insertCmd.Parameters.AddWithValue("id", newId);
            insertCmd.Parameters.AddWithValue("title", title);
            insertCmd.Parameters.AddWithValue("unit", unit);

            insertCmd.ExecuteNonQuery();

            Console.WriteLine($"Додано: {title} ({unit})");
        }

        conn.Close();
    }
}
