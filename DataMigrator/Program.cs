using System;
using System.Globalization;
using System.IO;
using System.Text;
using Microsoft.Extensions.Configuration;
using Npgsql;
using System.Collections.Generic;

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

        string csvFile = Path.Combine(AppContext.BaseDirectory, "C:\\Users\\user1\\Documents\\Tables\\Results.csv");

        InsertMeasurements(csvFile, targetConnStr);

        Console.WriteLine("Міграція Measurement завершена потужно!");
    }

    static void InsertMeasurements(string csvPath, string connStr)
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

        // Завантажуємо всі існуючі id_measurement, id_station та id_measured_unit
        var existingIds = new HashSet<string>();
        using (var cmd = new NpgsqlCommand("SELECT id_measurement FROM measurement", conn))
        using (var reader = cmd.ExecuteReader())
            while (reader.Read())
                existingIds.Add(reader.GetString(0));

        var existingStations = new HashSet<string>();
        using (var cmd = new NpgsqlCommand("SELECT id_station FROM station", conn))
        using (var reader = cmd.ExecuteReader())
            while (reader.Read())
                existingStations.Add(reader.GetString(0));

        var existingMeasuredUnits = new HashSet<string>();
        using (var cmd = new NpgsqlCommand("SELECT id_measured_unit FROM measured_unit", conn))
        using (var reader = cmd.ExecuteReader())
            while (reader.Read())
                existingMeasuredUnits.Add(reader.GetString(0));

        var writer = new StringBuilder();

        for (int i = 1; i < lines.Length; i++)
        {
            var values = lines[i].Split(',');

            if (values.Length < 5)
            {
                Console.WriteLine($"Пропускаємо рядок {i} – недостатньо колонок.");
                continue;
            }

            string idMeasurement = values[2].Trim();
            if (existingIds.Contains(idMeasurement))
            {
                Console.WriteLine($"Пропускаємо рядок {i} - id_measurement вже існує.");
                continue;
            }

            string timeCsv = values[0].Trim();

            // Обробка value
            string valueRaw = values[1].Trim().Trim('"');
            string valueCsv;
            if (string.IsNullOrWhiteSpace(valueRaw))
                valueCsv = "\\N"; // NULL
            else
            {
                // замінюємо кому на крапку для numeric
                valueRaw = valueRaw.Replace(',', '.');

                // перевіряємо, чи число валідне
                if (decimal.TryParse(valueRaw, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal val))
                    valueCsv = val.ToString(CultureInfo.InvariantCulture);
                else
                {
                    Console.WriteLine($"Пропускаємо рядок {i} - невірне число: {valueRaw}");
                    continue;
                }
            }

            // id_station
            string idStation = values[3].Trim();
            if (!existingStations.Contains(idStation))
                idStation = "\\N"; // NULL якщо немає у station

            // id_measured_unit
            string idMeasuredUnit = values[4].Trim();
            if (!existingMeasuredUnits.Contains(idMeasuredUnit))
                idMeasuredUnit = "\\N"; // NULL якщо немає у measured_unit

            writer.AppendLine($"{idMeasurement}\t{timeCsv}\t{valueCsv}\t{idStation}\t{idMeasuredUnit}");
            existingIds.Add(idMeasurement); // додаємо у HashSet
        }

        // COPY вставка
        using var copy = conn.BeginTextImport(
            "COPY measurement (id_measurement, time, value, id_station, id_measured_unit) " +
            "FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"
        );
        copy.Write(writer.ToString());
        copy.Close();

        conn.Close();
    }
}
