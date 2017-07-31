using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Xml.Linq;

namespace BackupTables
{
    public class Program
    {
        private static string GetDirectoryName(string filePath)
        {
            filePath = Path.GetFullPath(filePath);
            var dirName = Path.GetDirectoryName(filePath);
            if (dirName == null)
            {
                throw new IOException($"Path {filePath} has no parent directory");
            }
            return dirName;
        }

        private static string SaveBinary(SqlDataReader reader, int field, string parentFile, bool blobs)
        {
            if (!blobs)
            {
                return string.Empty;
            }

            var folder = GetDirectoryName(parentFile);
            var name = Guid.NewGuid() + ".blob";

            var buffer = new byte[0x10000];

            Console.WriteLine($"Saving blob to {name}");

            using (var logger = new ThrottledLogger())
            using (var stream = reader.GetStream(field))
            using (var file = new FileStream(Path.Combine(folder, name), FileMode.Create, FileAccess.Write))
            {
                int got;
                var total = 0;
                while ((got = stream.Read(buffer, 0, buffer.Length)) != 0)
                {
                    file.Write(buffer, 0, got);

                    total += got;
                    logger.Update($"Saved {total} bytes");
                }
            }

            Console.WriteLine($"Finished saving blob to {name}");
            return name;
        }

        private static void Export(string fileName, IEnumerable<string> tables, SqlConnection conn, bool blobs)
        {
            var rootElem = new XElement("tables");

            foreach (var table in tables)
            {
                var tableElem = new XElement("table");
                tableElem.SetAttributeValue("name", table);
                rootElem.Add(tableElem);

                Console.WriteLine($"Exporting table: {table}");
                var records = 0;

                using (var command = new SqlCommand(null, conn))
                {
                    command.CommandText = @"SELECT * FROM " + table;

                    using (var logger = new ThrottledLogger())
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            records++;

                            logger.Update($"Exported {records} records");

                            var recordElem = new XElement("record");
                            tableElem.Add(recordElem);

                            for (var f = 0; f < reader.FieldCount; f++)
                            {
                                var name = reader.GetName(f);
                                var type = reader.GetFieldType(f);
                                
                                var val = reader.IsDBNull(f) ? null : 
                                        type == typeof(byte[]) ? SaveBinary(reader, f, fileName, blobs) : 
                                        reader.GetValue(f);

                                var fieldElem = new XElement("field");
                                recordElem.Add(fieldElem);
                                fieldElem.SetAttributeValue("name", name);

                                if (type != null)
                                {
                                    fieldElem.SetAttributeValue("type", type.FullName);
                                }

                                if (val == null)
                                {
                                    fieldElem.SetAttributeValue("null", "true");
                                }
                                else
                                {
                                    var byteArray = val as byte[];
                                    fieldElem.Value = byteArray != null ? Convert.ToBase64String(byteArray) : Convert.ToString(val);
                                }
                            }
                        }
                    }

                    Console.WriteLine($"Finished table {table}, {records} records");
                }
            }

            File.WriteAllText(fileName, rootElem.ToString());
        }

        private static object FromString(string value, Type type, string parentFile)
        {
            if (type == typeof(Guid))
            {
                return Guid.Parse(value);
            }

            if (type == typeof(byte[]))
            {
                if (string.IsNullOrWhiteSpace(value))
                {
                    return new byte[0];
                }

                var folder = GetDirectoryName(parentFile);
                var fullPath = Path.Combine(folder, value);

                try
                {
                    return File.ReadAllBytes(fullPath);
                }
                catch (Exception x)
                {
                    Console.WriteLine($"WARNING: could not read {fullPath}, substituting empty byte array");
                    Console.WriteLine(x.GetBaseException().Message);
                    return new byte[0];
                }
            }

            return Convert.ChangeType(value, type);
        }

        private const string SqlGetDependencies = @"
SELECT
	Source = '[' + FK.TABLE_SCHEMA + '].[' + FK.TABLE_NAME + ']',
	Target = '[' + PK.TABLE_SCHEMA + '].[' + PK.TABLE_NAME + ']'    
FROM
    INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS C
INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS FK
    ON C.CONSTRAINT_NAME = FK.CONSTRAINT_NAME
INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK
    ON C.UNIQUE_CONSTRAINT_NAME = PK.CONSTRAINT_NAME
INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE CU
    ON C.CONSTRAINT_NAME = CU.CONSTRAINT_NAME
INNER JOIN (
            SELECT
                i1.TABLE_NAME,
                i2.COLUMN_NAME
            FROM
                INFORMATION_SCHEMA.TABLE_CONSTRAINTS i1
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE i2
                ON i1.CONSTRAINT_NAME = i2.CONSTRAINT_NAME
            WHERE
                i1.CONSTRAINT_TYPE = 'PRIMARY KEY'
           ) PT
    ON PT.TABLE_NAME = PK.TABLE_NAME
";

        private static void Import(string fileName, Dictionary<string, string> mappings, bool delete, SqlConnection conn)
        {
            Console.WriteLine($"Reading import file {fileName}...");

            var doc = XDocument.Load(fileName);
            
            var tableElements = doc.Descendants("table").ToList();

            string TableName(XElement table)
            {
                return (string)table.Attribute("name");
            }

            XElement TableByName(string name)
            {
                return tableElements.FirstOrDefault(e => TableName(e) == name);
            }
            
            var dependencies = new Dictionary<string, List<XElement>>();

            Console.WriteLine("Analyzing dependencies...");

            using (var command = new SqlCommand(null, conn))
            {
                command.CommandText = SqlGetDependencies;

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var source = reader.GetString(0);
                        var target = reader.GetString(1);

                        if (!dependencies.TryGetValue(source, out List<XElement> targets))
                        {
                            dependencies[source] = targets = new List<XElement>();
                        }

                        var targetTable = TableByName(target);

                        if (targetTable != null)
                        {
                            targets.Add(targetTable);
                        }                        
                    }
                }
            }
            
            IEnumerable<XElement> TableDependencies(XElement table)
            {
                if (dependencies.TryGetValue(TableName(table), out List<XElement> targets))
                {
                    return targets;
                }

                return Enumerable.Empty<XElement>();
            }

            var sortedTables = tableElements.TopologicalSort(TableDependencies).ToList();

            if (delete)
            {
                foreach (var tableElem in sortedTables)
                {
                    var table = (string)tableElem.Attribute("name");

                    Console.WriteLine($"Deleting from table: {table}");

                    using (var command = new SqlCommand(null, conn))
                    {
                        command.CommandText = @"DELETE FROM " + table;
                        var affected = command.ExecuteNonQuery();
                        Console.WriteLine("{0} - Deleted {1} rows", table, affected);
                    }
                }
            }

            sortedTables.Reverse();

            var exceptions = new List<(Exception error, string table, XElement record)>();

            foreach (var tableElem in sortedTables)
            {
                var table = (string)tableElem.Attribute("name");

                Console.WriteLine($"Reading schema of table: {table}");

                if (!mappings.TryGetValue(table, out string mappedTable))
                {
                    mappedTable = table;
                }

                var columnTypes = new Dictionary<string, SqlDbType>();
                var columnSizes = new Dictionary<string, int>();
                var columnPrecisions = new Dictionary<string, short>();
                var columnScales = new Dictionary<string, short>();

                using (var command = new SqlCommand(null, conn))
                {
                    command.CommandText = @"SELECT TOP 1 * FROM " + mappedTable;

                    using (var reader = command.ExecuteReader())
                    {
                        var schemaType = reader.GetSchemaTable();

                        foreach (var columnInfo in schemaType.Rows.OfType<DataRow>())
                        {
                            var columnName = (string)columnInfo["ColumnName"];
                          
                            columnTypes[columnName] = (SqlDbType)(int)columnInfo["ProviderType"];
                            columnSizes[columnName] = (int)columnInfo["ColumnSize"];
                            columnPrecisions[columnName] = (short)columnInfo["NumericPrecision"];
                            columnScales[columnName] = (short)columnInfo["NumericScale"];
                        }
                    }
                }

                var inserted = 0;
                var errors = 0;

                Console.WriteLine($"Importing table: {table}");

                using (var logger = new ThrottledLogger())
                {
                    foreach (var recordElem in tableElem.Descendants("record"))
                    {
                        using (var command = new SqlCommand(null, conn))
                        {
                            var columns = new List<string>();

                            foreach (var destColumn in columnTypes.Keys)
                            {
                                string sourceColumn;
                                if (!mappings.TryGetValue(destColumn, out sourceColumn))
                                {
                                    sourceColumn = destColumn;
                                }

                                var fieldElem = recordElem.Descendants("field").FirstOrDefault(f => (string)f.Attribute("name") == sourceColumn);

                                if (fieldElem == null)
                                {
                                    var sqlType = columnTypes[destColumn];
                                    var columnSize = columnSizes[destColumn];
                                    var columnPrecis = columnPrecisions[destColumn];
                                    var columnScale = columnScales[destColumn];

                                    object val = null;
                                    if (sqlType == SqlDbType.Bit)
                                    {
                                        val = 0;
                                    }
                                    else if (sqlType == SqlDbType.DateTime2)
                                    {
                                        val = DateTime.Now;
                                    }
                                    else if (sqlType == SqlDbType.UniqueIdentifier)
                                    {
                                        val = Guid.Empty;
                                    }

                                    if (val != null)
                                    {
                                        columns.Add(destColumn);
                                        command.Parameters.Add(new SqlParameter("@p" + destColumn, sqlType, columnSize)
                                        {
                                            Value = val,
                                            Precision = (byte)columnPrecis,
                                            Scale = (byte)columnScale
                                        });
                                    }
                                }
                                else
                                {
                                    var typeName = (string)fieldElem.Attribute("type");
                                    var isNull = !string.IsNullOrWhiteSpace((string)fieldElem.Attribute("null"));

                                    if (!isNull)
                                    {
                                        var value = FromString(fieldElem.Value, Type.GetType(typeName), fileName);
                                        var sqlType = columnTypes[destColumn];
                                        var columnSize = columnSizes[destColumn];
                                        var columnPrecis = columnPrecisions[destColumn];
                                        var columnScale = columnScales[destColumn];

                                        if (sqlType == SqlDbType.NVarChar)
                                        {
                                            columnSize = value?.ToString().Length ?? 0;
                                        }

                                        if (sqlType != SqlDbType.NVarChar || columnSize != 0)
                                        {
                                            columns.Add(destColumn);
                                            command.Parameters.Add(new SqlParameter("@p" + destColumn, sqlType, columnSize)
                                            {
                                                Value = value,
                                                Precision = (byte)columnPrecis,
                                                Scale = (byte)columnScale
                                            });
                                        }
                                    }
                                }
                            }

                            command.CommandText = @"INSERT INTO " + table + "(" +
                                                  string.Join(",", columns.Select(c => "[" + c + "]")) + ") VALUES (" +
                                                  string.Join(",", columns.Select(c => "@p" + c)) + ")";
                            command.Prepare();
                            try
                            {
                                inserted += command.ExecuteNonQuery();
                            }
                            catch (Exception x)
                            {
                                errors++;
                                x = x.GetBaseException();
                                exceptions.Add((x, table, recordElem));
                            }

                            logger.Update($"{inserted} records inserted, {errors} failures");
                        }
                    }
                }

                Console.WriteLine($"Finished importing {table}, {inserted} records, {errors} failures");
            }

            if (exceptions.Count != 0)
            {
                foreach (var exception in exceptions)
                {
                    Console.WriteLine($"In table {exception.table}: {exception.error.Message} - data was: {exception.record}");
                }
            }
        }

        private static int Usage()
        {
            Console.WriteLine();
            Console.WriteLine("Specify arguments as name=value pairs, e.g. f=text.xml");
            Console.WriteLine();
            Console.WriteLine("Required:");
            Console.WriteLine();
            Console.WriteLine("  f, filename");
            Console.WriteLine("  d, database");
            Console.WriteLine();
            Console.WriteLine("Optional:");
            Console.WriteLine();
            Console.WriteLine("  m, mode      -- import|export, default is export)");
            Console.WriteLine("  s, server    -- default is local");
            Console.WriteLine("  u, username  -- default is impersonation");
            Console.WriteLine("  p, password  -- required if username specified");
            Console.WriteLine("  t, tables    -- which tables to export (comma separated)");
            Console.WriteLine("  b, blobs     -- true|false, default is true, blobs are exported");
            Console.WriteLine("  c, clobber   -- true|false, default is false, import deletes all existing records");
            Console.WriteLine();
            return -1;
        }

        private static string GetValue(IReadOnlyDictionary<string, string> options, string key, string defaultValue)
        {
            return options.TryGetValue(key, out string value1) ? value1 :
                   options.TryGetValue(key.Substring(0, 1), out string value2) ? value2 : defaultValue;
        }

        public static int Main(string[] args)
        {
            var options = (

                from arg in args
                let split = arg.Split('=')
                where split.Length > 1
                select new {key = split[0], value = string.Join("=", split.Skip(1))}

            ).ToDictionary(p => p.key, p => p.value);

            var filename = GetValue(options, "filename", string.Empty);
            var database = GetValue(options, "database", string.Empty);            
            var mode = GetValue(options, "mode", "export");
            var server = GetValue(options, "server", ".");
            var username = GetValue(options, "username", string.Empty);
            var password = GetValue(options, "password", string.Empty);
            var clobber = GetValue(options, "clobber", string.Empty);
            var tables = GetValue(options, "tables", string.Empty).Split(',').Where(t => !string.IsNullOrWhiteSpace(t)).ToArray();
            var blobs = GetValue(options, "blobs", "true") != "false";

            if (filename.Length == 0 || database.Length == 0)
            {
                return Usage();
            }

            var security = username.Length == 0 ? "Integrated Security=true" : $"User Id={username};Password={password}";
            var connStr = $"Data Source={server};Initial Catalog={database};{security};MultipleActiveResultSets=True";

            using (var conn = new SqlConnection(connStr))
            {
                conn.Open();
                
                switch (mode)
                {
                    case "export":
                    case "e":
                        if (tables.Length == 0)
                        {
                            tables = conn.GetSchema("Tables").Select()
                                         .Where(r => r[3].ToString().ToLowerInvariant() != "view")
                                         .Select(r => $"[{r[1]}].[{r[2]}]").ToArray();
                        }
                        Export(filename, tables, conn, blobs);
                        break;

                    case "import":
                    case "i":
                        var mappings = new Dictionary<string, string>();
                        Import(filename, mappings, clobber == "true", conn);
                        break;

                    default:
                        return Usage();
                }
            }

            return 0;
        }
    }
}
