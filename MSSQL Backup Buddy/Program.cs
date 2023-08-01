using System;
using System.Data.SqlClient;
using System.IO;

namespace SqlServerBackup
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.BackgroundColor = ConsoleColor.Black;

            Console.ForegroundColor = ConsoleColor.Green;
            Console.Clear();
            Console.WriteLine("=========================================================  ");
            Console.WriteLine("     MSSQL BACKUP BUDDY - #UTIL5 ");
            Console.WriteLine("=========================================================  ");
            Console.ForegroundColor = ConsoleColor.DarkYellow;
            Console.WriteLine("     SIMPHONY LOCAL DATABASE BACKUP UTILITY");
            Console.WriteLine("=========================================================  ");
            Console.WriteLine("");

            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("=========================================================  ");
            Console.WriteLine("# DEVELOPER :- GOHULAN SOMANATHAN ");
            Console.WriteLine("=========================================================  ");
            Console.WriteLine("# COMPANY   :- HOSPITALITY TECHNOLOGY");
            Console.WriteLine("=========================================================  ");
            Console.WriteLine("# APP VER   :- 1.0.0 RELE 2023 APRIL 24");
            Console.WriteLine("=========================================================  ");
            Console.WriteLine("");

            Console.ForegroundColor = ConsoleColor.White;
            string serverName = "localhost\\sqlexpress";
            string database1 = "datastore";
            string database2 = "checkpostingdb";
            string backupFolderPath = @"C:\BackupBuddy\";



            // Create the backup folder if it doesn't exist
            Directory.CreateDirectory(backupFolderPath);

            // Replace YOUR_SQL_SERVER_NAME with the actual server name or instance name



                    string connectionString = $"Data Source={serverName};User Id=sa;Password=#1mymicros;";

                    try
                    {
                        using (SqlConnection connection = new SqlConnection(connectionString))
                        {
                            connection.Open();

                            // Perform backup for database1
                            BackupDatabase(connection, database1, backupFolderPath);

                            // Perform backup for database2
                            BackupDatabase(connection, database2, backupFolderPath);
                        }

                        Console.WriteLine("Backups completed successfully!");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Error occurred: " + ex.Message);
                    }

                    


        }


   

        static void BackupDatabase(SqlConnection connection, string databaseName, string backupFolderPath)
        {
            string dateString = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");

            string backupFileName = $"{backupFolderPath}{databaseName}_" + dateString.Replace(":", "_").Replace(" ", "_") + " .sg";

            string backupQuery = $"BACKUP DATABASE [{databaseName}] TO DISK = '{backupFileName}' WITH FORMAT, STATS = 10;";
            string logFileName = $"{backupFolderPath}{databaseName}_backup_log_" + dateString.Replace(":", "_").Replace(" ", "_") + ".txt";
            string errorlogfile = $"{backupFolderPath}{databaseName}_error_" + dateString.Replace(":", "_").Replace(" ", "_") + ".txt";

            try
            {
                using (SqlCommand cmd = new SqlCommand(backupQuery, connection))
                {
                    cmd.ExecuteNonQuery();
                }

                // Log backup details to the log file
                LogBackupDetails(connection, databaseName, logFileName);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error occurred while backing up {databaseName}: {ex.Message}");
            }
        }

        static void LogBackupDetails(SqlConnection connection, string databaseName, string logFileName)
        {
            string logQuery = $"USE [{databaseName}]; SELECT t.name, SUM(p.rows) AS [RowCount], SUM(a.total_pages * 8) AS [TotalSizeKB] FROM sys.tables t INNER JOIN sys.indexes i ON t.object_id = i.object_id INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id WHERE t.is_ms_shipped = 0 GROUP BY t.name;";

            string textData = "";

            // Create the text paragraphs you want to add
            string paragraph1 = "=========================================================  \n";
            string paragraph2 = "     SIMPHONY - MSSQL BACKUP BUDDY - #UTIL5  \n";
            string paragraph3 = "=========================================================  \n";
            string paragraph12 = "# BACKUP DATE :- MSSQL - " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "   \n";
            string paragraph4 = "=========================================================  \n";
            string paragraph5 = "# DEVELOPER :- GOHULAN SOMANATHAN \n";
            string paragraph6 = "=========================================================  \n";
            string paragraph7 = "# COMPANY   :- HOSPITALITY TECHNOLOGY\n";
            string paragraph8 = "=========================================================  \n";
            string paragraph9 = "# APP VER   :- 1.0.0 RELE 2023 APRIL 24\n";
            string paragraph10 = "=========================================================  \n";
            string paragraph11 = "\n";
            // Method 1: Using concatenation
            textData = paragraph1 + paragraph2 + paragraph3 + paragraph12 + paragraph4 + paragraph5 + paragraph6 + paragraph7 + paragraph8 + paragraph9 + paragraph10 + paragraph11;


            try
            {
                using (SqlCommand cmd = new SqlCommand(logQuery, connection))
                {
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        using (StreamWriter writer = new StreamWriter(logFileName))
                        {
                            writer.WriteLine(textData);
                            writer.WriteLine($"Backup details for database: {databaseName} \n");



                            while (reader.Read())
                            {
                                string tableName = reader["name"].ToString();
                                int rowCount = Convert.ToInt32(reader["RowCount"]);
                                long totalSizeKB = Convert.ToInt64(reader["TotalSizeKB"]);


                                writer.WriteLine($"Table: {tableName}, Row Count: {rowCount}, Size(KB): {totalSizeKB}");
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error occurred while logging backup details for {databaseName}: {ex.Message}");
                

            }
        }
    }
}
