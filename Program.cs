using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Xml.Linq;

namespace BackupTables
{
    public class Program
    {
        public static void WithConnection(string connectionStringName, Action<SqlConnection> action)
        {
            using (var conn = new SqlConnection(ConfigurationManager.ConnectionStrings[connectionStringName].ConnectionString))
            {
                conn.Open();
                action(conn);
            }
        }
        
        private static void Export(string fileName, IEnumerable<string> tables, SqlConnection conn)
        {
            var logger = new ThrottledLogger();
            var records = 0;
    
            var rootElem = new XElement("tables");

            foreach (var table in tables)
            {
                var tableElem = new XElement("table");
                tableElem.SetAttributeValue("name", table);
                rootElem.Add(tableElem);

                Console.WriteLine($"Exporting table: {table}");
                records = 0;

                using (var command = new SqlCommand(null, conn))
                {
                    command.CommandText = @"SELECT * FROM " + table;

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            records++;

                            logger.Add($"-- exported {records} records so far");

                            var recordElem = new XElement("record");
                            tableElem.Add(recordElem);

                            for (var f = 0; f < reader.FieldCount; f++)
                            {
                                var name = reader.GetName(f);
                                var type = reader.GetFieldType(f);
                                var val = reader.IsDBNull(f) ? null : reader.GetValue(f);

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

        private static object FromString(string value, Type type)
        {
            if (type == typeof(Guid))
            {
                return Guid.Parse(value);
            }

            if (type == typeof(byte[]))
            {
                try
                {
                    return Convert.FromBase64String(value);
                }
                catch (Exception)
                {
                    Console.WriteLine("WARNING: could not interpret value as base64, substituting empty byte array");
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
            var logger = new ThrottledLogger();

            Console.WriteLine($"Reading import file {fileName}...");

            var doc = XDocument.Load(fileName);
            
            var tableElements = doc.Descendants("table");

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

            foreach (var tableElem in sortedTables)
            {
                var table = (string)tableElem.Attribute("name");

                Console.WriteLine($"Reading schema of table: {table}");

                string mappedTable;

                if (!mappings.TryGetValue(table, out mappedTable))
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

                Console.WriteLine($"Importing table: {table}");

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
                                    var value = FromString(fieldElem.Value, Type.GetType(typeName));
                                    var sqlType = columnTypes[destColumn];
                                    var columnSize = columnSizes[destColumn];
                                    var columnPrecis = columnPrecisions[destColumn];
                                    var columnScale = columnScales[destColumn];
                                    
                                    if (sqlType == SqlDbType.NVarChar)
                                    {
                                        columnSize = value != null ? value.ToString().Length : 0;
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
                        inserted += command.ExecuteNonQuery();

                        logger.Add($"-- imported {inserted} records so far");
                    }
                }

                Console.WriteLine($"Finished importing {table}, {inserted} records");
            }
        }

        public static void Main(string[] args)
        {
            var mode = args[1];
            var fileName = args[2];
            var delete = args.Length >= 4 && args[3] == "delete";

            WithConnection(args[0], conn =>
            {
                switch (mode)
                {
                    case "export":
                        string[] tables;
                        if (args.Length == 3)
                        {
                            tables = conn.GetSchema("Tables").Select()
                                         .Where(r => r[3].ToString().ToLowerInvariant() != "view")
                                         .Select(r => $"[{r[1]}].[{r[2]}]").ToArray();
                        }
                        else
                        {
                            tables = args[3].Split(',');
                        }
                        
                        Export(fileName, tables, conn);
                        break;

                    case "import":
                        //var mappings = (args.Length < 4 ? string.Empty : args[3]).Split(',')
                        //                    .Select(m => m.Split('='))
                        //                    .ToDictionary(m => m[0], m => m[1]);
                        var mappings = new Dictionary<string, string>();
                        Import(fileName, mappings, delete, conn);
                        break;
                }
            });
        }
    }
}
