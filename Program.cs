using System;
using System.IO;
using System.Text.Json;
using System.Text.RegularExpressions;
using FirebirdSql.Data.FirebirdClient;

namespace DbMetaTool
{
    public static class Program
    {
        // Przykładowe wywołania:
        // DbMetaTool build-db --db-dir "C:\db\fb5" --scripts-dir "C:\scripts"
        // DbMetaTool export-scripts --connection-string "..." --output-dir "C:\out"
        // DbMetaTool update-db --connection-string "..." --scripts-dir "C:\scripts"
        public static int Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine("Użycie:");
                Console.WriteLine("  build-db --db-dir <ścieżka> --scripts-dir <ścieżka>");
                Console.WriteLine("  export-scripts --connection-string <connStr> --output-dir <ścieżka>");
                Console.WriteLine("  update-db --connection-string <connStr> --scripts-dir <ścieżka>");
                return 1;
            }

            try
            {
                var command = args[0].ToLowerInvariant();

                switch (command)
                {
                    case "build-db":
                        {
                            string dbDir = GetArgValue(args, "--db-dir");
                            string scriptsDir = GetArgValue(args, "--scripts-dir");

                            BuildDatabase(dbDir, scriptsDir);
                            Console.WriteLine("Baza danych została zbudowana pomyślnie.");
                            return 0;
                        }

                    case "export-scripts":
                        {
                            string connStr = GetArgValue(args, "--connection-string");
                            string outputDir = GetArgValue(args, "--output-dir");

                            ExportScripts(connStr, outputDir);
                            Console.WriteLine("Skrypty zostały wyeksportowane pomyślnie.");
                            return 0;
                        }

                    case "update-db":
                        {
                            string connStr = GetArgValue(args, "--connection-string");
                            string scriptsDir = GetArgValue(args, "--scripts-dir");

                            UpdateDatabase(connStr, scriptsDir);
                            Console.WriteLine("Baza danych została zaktualizowana pomyślnie.");
                            return 0;
                        }

                    default:
                        Console.WriteLine($"Nieznane polecenie: {command}");
                        return 1;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Błąd: " + ex.Message);
                return -1;
            }
        }

        private static string GetArgValue(string[] args, string name)
        {
            int idx = Array.IndexOf(args, name);
            if (idx == -1 || idx + 1 >= args.Length)
                throw new ArgumentException($"Brak wymaganego parametru {name}");
            return args[idx + 1];
        }

        /// <summary>
        /// Buduje nową bazę danych Firebird 5.0 na podstawie skryptów.
        /// </summary>
        public static void BuildDatabase(string databaseDirectory, string scriptsDirectory)
        {
            // TODO:
            // 1) Utwórz pustą bazę danych FB 5.0 w katalogu databaseDirectory.
            // 2) Wczytaj i wykonaj kolejno skrypty z katalogu scriptsDirectory
            //    (tylko domeny, tabele, procedury).
            // 3) Obsłuż błędy i wyświetl raport.

            Directory.CreateDirectory(databaseDirectory);
            // Ścieżka do pliku bazy
            string dbPath = Path.Combine(databaseDirectory, "NewDatabase.fdb");
            // Connection string do nowej bazy
            string connectionString =
                $"User=SYSDBA;Password=masterkey;Database={dbPath};DataSource=localhost;Port=3050;Dialect=3;";

            try
            {
                if (File.Exists(dbPath))
                {
                    File.Delete(dbPath);
                }

                FbConnection.CreateDatabase(connectionString, 4096, true, true);

                using var connection = new FbConnection(connectionString);
                connection.Open();

                // Wczytaj i wykonaj skrypty w kolejności: domeny -> tabele -> procedury
                var domainFiles = Directory.GetFiles(scriptsDirectory, "*.sql")
                    .Where(f => Path.GetFileName(f).Contains("domain", StringComparison.OrdinalIgnoreCase));

                var tableFiles = Directory.GetFiles(scriptsDirectory, "*.sql")
                    .Where(f => Path.GetFileName(f).Contains("table", StringComparison.OrdinalIgnoreCase));

                var procedureFiles = Directory.GetFiles(scriptsDirectory, "*.sql")
                    .Where(f => Path.GetFileName(f).Contains("procedure", StringComparison.OrdinalIgnoreCase));

                // Domeny
                foreach (var file in domainFiles)
                {
                    ExecuteSqlFile(file, connection, databaseDirectory);
                }

                // Tabele
                foreach (var file in tableFiles)
                {
                    ExecuteSqlFile(file, connection, databaseDirectory);
                }

                // Procedury
                foreach (var file in procedureFiles)
                {
                    ExecuteSqlFile(file, connection, databaseDirectory, isProcedure:true);
                }

                // Raport
                File.WriteAllText(Path.Combine(databaseDirectory, "report.txt"),
                    $"Database created: {dbPath}\nScripts executed from directory: {scriptsDirectory}");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error while building database: " + ex.Message);
                File.WriteAllText(Path.Combine(databaseDirectory, "error.log"), ex.ToString());
            }
        }

        static void ExecuteSqlFile(string file, FbConnection connection, string databaseDirectory, bool isProcedure = false)
        {
            string script = File.ReadAllText(file);

            if (isProcedure)
            {
                // Procedury wykonujemy jako całość
                using var cmd = new FbCommand(script, connection);
                try
                {
                    cmd.ExecuteNonQuery();
                    Console.WriteLine($"Procedures executed from file: {Path.GetFileName(file)}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error in {Path.GetFileName(file)}: {ex.Message}");
                    File.AppendAllText(Path.Combine(databaseDirectory, "error.log"),
                        $"[{DateTime.Now}] {Path.GetFileName(file)}: {ex}\n");
                }
            }
            else
            {
                // Domeny i tabele dzielimy po średnikach
                var commands = script.Split(';', StringSplitOptions.RemoveEmptyEntries);
                foreach (var command in commands)
                {
                    string sql = command.Trim();
                    if (string.IsNullOrWhiteSpace(sql)) continue;

                    using var cmd = new FbCommand(sql, connection);
                    try
                    {
                        cmd.ExecuteNonQuery();
                        Console.WriteLine($"Executed: {Path.GetFileName(file)}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error in {Path.GetFileName(file)}: {ex.Message}");
                        File.AppendAllText(Path.Combine(databaseDirectory, "error.log"),
                            $"[{DateTime.Now}] {Path.GetFileName(file)}: {ex}\n");
                    }
                }
            }
        }

        /// <summary>
        /// Generuje skrypty metadanych z istniejącej bazy danych Firebird 5.0.
        /// </summary>
        public static void ExportScripts(string connectionString, string outputDirectory)
        {
            // TODO:
            // 1) Połącz się z bazą danych przy użyciu connectionString.
            // 2) Pobierz metadane domen, tabel (z kolumnami) i procedur.
            // 3) Wygeneruj pliki .sql / .json / .txt w outputDirectory.

            Directory.CreateDirectory(outputDirectory);
            var domains = new List<string>();
            var tables = new List<TableInfo>();
            var procedures = new List<ProcedureInfo>();

            try
            {
                using var connection = new FbConnection(connectionString);
                connection.Open();

                // DOMAINS
                string sqlDomains = @"
                    SELECT RDB$FIELD_NAME,
                        RDB$FIELD_TYPE, RDB$CHARACTER_LENGTH,
                        RDB$CHARACTER_SET_ID,
                        RDB$VALIDATION_SOURCE,
                        RDB$NULL_FLAG
                    FROM RDB$FIELDS
                    WHERE RDB$FIELD_NAME NOT LIKE 'RDB$%' AND RDB$SYSTEM_FLAG = 0";
                using var cmdDomains = new FbCommand(sqlDomains, connection);
                using var readerDomains = cmdDomains.ExecuteReader();
                while (readerDomains.Read())
                {
                    string domainName = readerDomains.GetString(0).Trim();
                    int fieldType = readerDomains.GetInt16(1);
                    int charLength = readerDomains.IsDBNull(2) ? 0 : readerDomains.GetInt16(2);
                    int charSetId = readerDomains.IsDBNull(3) ? 0 : readerDomains.GetInt16(3);
                    string validationSource = readerDomains.IsDBNull(4) ? string.Empty : readerDomains.GetString(4).Trim();
                    bool notNull = !readerDomains.IsDBNull(5) && readerDomains.GetInt16(5) == 1;

                    string sqlType = fieldType switch
                    {
                        7  => "SMALLINT",
                        8  => "INTEGER",
                        10 => "FLOAT",
                        12 => "DATE",
                        13 => "TIME",
                        14 => $"CHAR({charLength})",
                        16 => "BIGINT",
                        27 => "DOUBLE PRECISION",
                        35 => "TIMESTAMP",
                        37 => $"VARCHAR({charLength})", // charset trzeba zmapować osobno
                        261 => "BLOB",
                        _  => "UNKNOWN"
                    };

                    // pobierz inne właściwości i zapisz do listy
                    string createDomain = $"CREATE DOMAIN {domainName} AS {sqlType}";

                    if (notNull)
                    {
                        createDomain += " NOT NULL";
                    }
                    if (!string.IsNullOrEmpty(validationSource))
                    {
                        createDomain += $" {validationSource}";
                    }
                    domains.Add(createDomain + ";");
                }

                //TABLES
                string sqlTables = @"
                    SELECT RDB$RELATION_NAME
                    FROM RDB$RELATIONS
                    WHERE RDB$SYSTEM_FLAG = 0 AND RDB$VIEW_BLR IS NULL";
                using var cmdTables = new FbCommand(sqlTables, connection);
                using var readerTables = cmdTables.ExecuteReader();

                while (readerTables.Read())
                {
                    string tableName = readerTables.GetString(0).Trim();
                    var columns = new List<string>();
                    // pobierz kolumny i zapisz do listy
                    string sqlColumns = @"
                        SELECT 
                            rf.RDB$FIELD_NAME,
                            f.RDB$FIELD_TYPE,
                            f.RDB$CHARACTER_LENGTH,
                            f.RDB$FIELD_PRECISION,
                            f.RDB$FIELD_SUB_TYPE,
                            rf.RDB$NULL_FLAG,
                            rf.RDB$DEFAULT_SOURCE
                        FROM RDB$RELATION_FIELDS rf
                        JOIN RDB$FIELDS f ON rf.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME
                        WHERE rf.RDB$RELATION_NAME = @tableName
                        ORDER BY rf.RDB$FIELD_POSITION";

                    using var cmdColumns = new FbCommand(sqlColumns, connection);
                    cmdColumns.Parameters.AddWithValue("@tableName", tableName);

                    using var readerColumns = cmdColumns.ExecuteReader();

                    while (readerColumns.Read())
                    {
                        string columnName = readerColumns.GetString(0).Trim();
                        short fieldType = readerColumns.GetInt16(1);
                        int charLength = readerColumns.IsDBNull(2) ? 0 : readerColumns.GetInt32(2);
                        int precision = readerColumns.IsDBNull(3) ? 0 : readerColumns.GetInt32(3);
                        short subType = readerColumns.IsDBNull(4) ? (short)0 : readerColumns.GetInt16(4);
                        bool notNull = !readerColumns.IsDBNull(5) && readerColumns.GetInt16(5) == 1;
                        string? defaultSource = readerColumns.IsDBNull(6) ? null : readerColumns.GetString(6).Trim();

                        string sqlType = fieldType switch
                        {
                            7  => "SMALLINT",
                            8  => "INTEGER",
                            10 => "FLOAT",
                            12 => "DATE",
                            13 => "TIME",
                            14 => "CHAR(" + charLength + ")",
                            16 => subType == 1 ? $"NUMERIC({precision},{charLength})" : $"BIGINT",
                            27 => "DOUBLE PRECISION",
                            35 => "TIMESTAMP",
                            37 => "VARCHAR(" + charLength + ")",
                            261 => "BLOB",
                            _  => "UNKNOWN"
                        };

                        string columnDef = $"{columnName} {sqlType}" +
                                        (notNull ? " NOT NULL" : "") +
                                        (defaultSource != null ? $" {defaultSource}" : "");

                        columns.Add(columnDef);
                    }

                    // Pobierz constraints
                    var constraints = new List<string>();
                    string sqlConstraints = @"
                        SELECT rc.RDB$CONSTRAINT_TYPE,
                            rc.RDB$CONSTRAINT_NAME,
                            iseg.RDB$FIELD_NAME,
                            ref.RDB$CONST_NAME_UQ,
                            rc2.RDB$RELATION_NAME AS REFERENCED_TABLE,
                            iseg2.RDB$FIELD_NAME AS REFERENCED_FIELD
                        FROM RDB$RELATION_CONSTRAINTS rc
                        JOIN RDB$INDEX_SEGMENTS iseg ON rc.RDB$INDEX_NAME = iseg.RDB$INDEX_NAME
                        LEFT JOIN RDB$REF_CONSTRAINTS ref ON rc.RDB$CONSTRAINT_NAME = ref.RDB$CONSTRAINT_NAME
                        LEFT JOIN RDB$RELATION_CONSTRAINTS rc2 ON ref.RDB$CONST_NAME_UQ = rc2.RDB$CONSTRAINT_NAME
                        LEFT JOIN RDB$INDEX_SEGMENTS iseg2 ON rc2.RDB$INDEX_NAME = iseg2.RDB$INDEX_NAME
                        WHERE rc.RDB$RELATION_NAME = @tableName";

                    using var cmdConstraints = new FbCommand(sqlConstraints, connection);
                    cmdConstraints.Parameters.AddWithValue("@tableName", tableName);

                    using var readerConstraints = cmdConstraints.ExecuteReader();
                    while (readerConstraints.Read())
                    {
                        string type = readerConstraints.GetString(0).Trim();
                        string name = readerConstraints.GetString(1).Trim();
                        string field = readerConstraints.GetString(2).Trim();

                        switch (type)
                        {
                            case "PRIMARY KEY":
                                constraints.Add($"CONSTRAINT {name} PRIMARY KEY ({field})");
                                break;
                            case "UNIQUE":
                                constraints.Add($"CONSTRAINT {name} UNIQUE ({field})");
                                break;
                            case "FOREIGN KEY":
                                {
                                    string referencedTable = readerConstraints.GetString(4).Trim();
                                    string referencedField = readerConstraints.GetString(5).Trim();
                                    constraints.Add($"CONSTRAINT {name} FOREIGN KEY ({field}) REFERENCES {referencedTable}({referencedField})");
                                    break;
                                }
                                
                            case "CHECK":
                                constraints.Add($"CONSTRAINT {name} CHECK (...)");
                                break;
                        }
                    }

                    tables.Add(new TableInfo
                    {
                        Name = tableName,
                        Columns = columns,
                        Constraints = constraints
                    });
                }

                //PROCEDURES

                string sqlProcedures = @"
                    SELECT RDB$PROCEDURE_NAME, RDB$PROCEDURE_SOURCE
                    FROM RDB$PROCEDURES
                    WHERE RDB$SYSTEM_FLAG = 0";
                
                using var cmdProcedures = new FbCommand(sqlProcedures, connection);
                using var readerProcedures = cmdProcedures.ExecuteReader();
                while (readerProcedures.Read())
                {
                    string procedureName = readerProcedures.GetString(0).Trim();
                    string procSource = readerProcedures.IsDBNull(1) ? string.Empty : readerProcedures.GetString(1).Trim();
                    // pobierz parametry i ciało procedury, zapisz do listy

                    string sqlParams = @"
                        SELECT p.RDB$PARAMETER_NAME, p.RDB$PARAMETER_TYPE, f.RDB$FIELD_TYPE, f.RDB$CHARACTER_LENGTH
                        FROM RDB$PROCEDURE_PARAMETERS p
                        JOIN RDB$FIELDS f ON p.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME
                        WHERE p.RDB$PROCEDURE_NAME = @procedureName
                        ORDER BY p.RDB$PARAMETER_NUMBER";
                    
                    using var cmdParams = new FbCommand(sqlParams, connection);
                    cmdParams.Parameters.AddWithValue("@procedureName", procedureName);
                    var parameters = new List<ProcedureParameter>();
                    using var readerParams = cmdParams.ExecuteReader();
                    while (readerParams.Read())
                    {
                        string paramName = readerParams.GetString(0).Trim();
                        short paramType = readerParams.GetInt16(1); // 0 = input, 1 = output
                        int fieldType = readerParams.GetInt16(2);
                        int charLength = readerParams.IsDBNull(3) ? 0 : readerParams.GetInt16(3);

                        string sqlType = fieldType switch
                        {
                            7  => "SMALLINT",
                            8  => "INTEGER",
                            10 => "FLOAT",
                            12 => "DATE",
                            13 => "TIME",
                            14 => $"CHAR({charLength})",
                            16 => "BIGINT",
                            27 => "DOUBLE PRECISION",
                            35 => "TIMESTAMP",
                            37 => $"VARCHAR({charLength})",
                            261 => "BLOB",
                            _  => "UNKNOWN"
                        };

                        parameters.Add(new ProcedureParameter
                        {
                            Name = paramName,
                            Type = sqlType,
                            IsOutput = paramType == 1
                        });
                    }

                    procedures.Add(new ProcedureInfo
                    {
                        Name = procedureName,
                        Source = procSource,
                        Parameters = parameters
                    });
                }
            }
            catch (Exception ex)
            {
                File.WriteAllText(Path.Combine(outputDirectory, "error.log"), ex.ToString());
                return;
            }

            // Zapisz domeny, tabele i procedury do plików w outputDirectory
            File.WriteAllText(Path.Combine(outputDirectory, "domains.sql"), string.Join(Environment.NewLine, domains));

            var tableSql = new List<string>();
            foreach (var table in tables)
            {
                tableSql.Add($"CREATE TABLE {table.Name} (\n    " +
                             string.Join(",\n    ", table.Columns.Concat(table.Constraints ?? Enumerable.Empty<string>())) +
                             "\n);");
            }
            
            File.WriteAllText(Path.Combine(outputDirectory, "tables.sql"), string.Join(Environment.NewLine, tableSql));

            var procSql = new List<string>();
            int counter = 1;
            foreach (var proc in procedures)
            {
                string paramList = "";
                string returnsList = "";

                if(proc.Parameters != null)
                {
                    var inParams = proc.Parameters.Where(p => !p.IsOutput).Select(p => $"{p.Name} {p.Type}");
                    var outParams = proc.Parameters.Where(p => p.IsOutput).Select(p => $"{p.Name} {p.Type}");

                    paramList = inParams.Any() ? $"({string.Join(", ", inParams)})" : "";
                    returnsList = outParams.Any() ? $"RETURNS ({string.Join(", ", outParams)})" : "";
                }
                
                string sql = $"CREATE OR ALTER PROCEDURE {proc.Name}";

                if (!string.IsNullOrEmpty(paramList))
                    sql += $" {paramList}";

                if (!string.IsNullOrEmpty(returnsList))
                    sql += $" {returnsList}";

                sql += $" AS\n{proc.Source}";

                procSql.Add(sql);

                File.WriteAllText(Path.Combine(outputDirectory, $"procedures{counter}.sql"), string.Join(Environment.NewLine, sql));
                counter++;
            }
        }

        /// <summary>
        /// Aktualizuje istniejącą bazę danych Firebird 5.0 na podstawie skryptów.
        /// </summary>
        public static void UpdateDatabase(string connectionString, string scriptsDirectory)
        {
            // TODO:
            // 1) Połącz się z bazą danych przy użyciu connectionString.
            // 2) Wykonaj skrypty z katalogu scriptsDirectory (tylko obsługiwane elementy).
            // 3) Zadbaj o poprawną kolejność i bezpieczeństwo zmian.
            if (!Directory.Exists(scriptsDirectory))
                throw new DirectoryNotFoundException($"Scripts directory not found: {scriptsDirectory}");

            using var connection = new FbConnection(connectionString);
            connection.Open();
            
            //transakcja dla bezpieczenstwa
            using var transaction = connection.BeginTransaction();
            try
            {
                // Wczytaj i wykonaj skrypty w kolejności: domeny -> tabele -> procedury
                var domainFiles = Directory.GetFiles(scriptsDirectory, "*.sql")
                    .Where(f => Path.GetFileName(f).Contains("domain", StringComparison.OrdinalIgnoreCase));

                var tableFiles = Directory.GetFiles(scriptsDirectory, "*.sql")
                    .Where(f => Path.GetFileName(f).Contains("table", StringComparison.OrdinalIgnoreCase));

                var procedureFiles = Directory.GetFiles(scriptsDirectory, "*.sql")
                    .Where(f => Path.GetFileName(f).Contains("procedure", StringComparison.OrdinalIgnoreCase));

                // Domeny
                foreach (var file in domainFiles)
                {
                    ExecuteSqlFileInUpdate(file, connection, transaction);
                }

                // Tabele
                foreach (var file in tableFiles)
                {
                    ExecuteSqlFileInUpdate(file, connection, transaction);
                }

                // Procedury
                foreach (var file in procedureFiles)
                {
                    ExecuteSqlFileInUpdate(file, connection, transaction, isProcedure:true);
                }

                transaction.Commit();
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                throw new InvalidOperationException("Database update failed.", ex);
            }
        }
        static void ExecuteSqlFileInUpdate(string file, FbConnection connection, FbTransaction transaction, bool isProcedure = false)
        {
            string script = File.ReadAllText(file);

            if (isProcedure)
            {
                // Procedury wykonujemy jako całość
                using var cmd = new FbCommand(script, connection, transaction);
                try
                {
                    cmd.ExecuteNonQuery();
                    Console.WriteLine($"Executed procedures from file: {Path.GetFileName(file)}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error in {Path.GetFileName(file)}: {ex.Message}");
                }
            }
            else
            {
                // Domeny i tabele dzielimy po średnikach
                var commands = script.Split(';', StringSplitOptions.RemoveEmptyEntries);
                foreach (var command in commands)
                {
                    string sql = command.Trim();
                    if (string.IsNullOrWhiteSpace(sql)) continue;

                    using var cmd = new FbCommand(sql, connection, transaction);
                    try
                    {
                        cmd.ExecuteNonQuery();
                        Console.WriteLine($"Executed: {Path.GetFileName(file)}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error in {Path.GetFileName(file)}: {ex.Message}");
                    }
                }
            }
        }
    }
}


public class TableInfo
{
    public required string Name { get; set; }
    public required List<string> Columns { get; set; }
    public List<string>? Constraints { get; set; }
}

public class ProcedureInfo
{
    public required string Name { get; set; }
    public required string Source { get; set; }
    public List<ProcedureParameter>? Parameters { get; set; }
}

public class ProcedureParameter
{
    public required string Name { get; set; }
    public required string Type { get; set; }
    public bool IsOutput { get; set; }
}
