using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using System.Xml.Serialization;
using Microsoft.SqlServer.Server;

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
            var rootElem = new XElement("tables");

            foreach (var table in tables)
            {
                var tableElem = new XElement("table");
                tableElem.SetAttributeValue("name", table);
                rootElem.Add(tableElem);

                using (var command = new SqlCommand(null, conn))
                {
                    command.CommandText = @"SELECT * FROM " + table;

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
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
                                    fieldElem.Value = Convert.ToString(val);
                                }
                            }
                        }
                    }
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

            return Convert.ChangeType(value, type);
        }

        private static void Import(string fileName, Dictionary<string, string> mappings, bool delete, SqlConnection conn)
        {
            var doc = XDocument.Load(fileName);

            if (delete)
            {
                foreach (var tableElem in doc.Descendants("table").Reverse())
                {
                    var table = (string)tableElem.Attribute("name");

                    using (var command = new SqlCommand(null, conn))
                    {
                        command.CommandText = @"DELETE FROM " + table;
                        var affected = command.ExecuteNonQuery();
                        Console.WriteLine("{0} - Deleted {1} rows", table, affected);
                    }
                }
            }

            foreach (var tableElem in doc.Descendants("table"))
            {
                var table = (string)tableElem.Attribute("name");

                string mappedTable;

                if (!mappings.TryGetValue(table, out mappedTable))
                {
                    mappedTable = table;
                }

                var columnTypes = new Dictionary<string, SqlDbType>();
                var columnSizes = new Dictionary<string, int>();

                using (var command = new SqlCommand(null, conn))
                {
                    command.CommandText = @"SELECT TOP 1 * FROM " + mappedTable;

                    using (var reader = command.ExecuteReader())
                    {
                        var schemaType = reader.GetSchemaTable();

                        foreach (var columnInfo in schemaType.Rows.OfType<DataRow>())
                        {
                            var columnName = (string)columnInfo["ColumnName"];
                            var columnType = (SqlDbType)(int)columnInfo["ProviderType"];
                            var columnSize = (int)columnInfo["ColumnSize"];

                            columnTypes[columnName] = columnType;
                            columnSizes[columnName] = columnSize;
                        }
                    }
                }

                var inserted = 0;

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
                                    command.Parameters.Add(new SqlParameter("@p" + destColumn, sqlType, columnSize) { Value = val });
                                }
                            }
                            else
                            {
                                var typeName = (string)fieldElem.Attribute("type");
                                var isNull = !string.IsNullOrWhiteSpace((string)fieldElem.Attribute("null"));

                                if (!isNull)
                                {
                                    columns.Add(destColumn);

                                    var value = FromString(fieldElem.Value, Type.GetType(typeName));
                                    var sqlType = columnTypes[destColumn];
                                    var columnSize = columnSizes[destColumn];

                                    if (sqlType == SqlDbType.NVarChar)
                                    {
                                        columnSize = value != null ? value.ToString().Length : 0;
                                    }

                                    command.Parameters.Add(new SqlParameter("@p" + destColumn, sqlType, columnSize) { Value = value });
                                }
                            }
                        }

                        command.CommandText = @"INSERT INTO " + table + "(" +
                                              string.Join(",", columns.Select(c => "[" + c + "]")) + ") VALUES (" +
                                              string.Join(",", columns.Select(c => "@p" + c)) + ")";
                        command.Prepare();
                        inserted += command.ExecuteNonQuery();
                    }
                }

                Console.WriteLine("{0} - Inserted {1} rows", table, inserted);
            }
        }

        public static void Main(string[] args)
        {
            var mode = args[1];
            var fileName = args[2];
            var delete = args.Length >= 5 && args[4] == "delete";

            WithConnection(args[0], conn =>
            {
                switch (mode)
                {
                    case "export":
                        var tables = args[3].Split(',');
                        Export(fileName, tables, conn);
                        break;

                    case "import":
                        var mappings = (args.Length < 4 ? string.Empty : args[3]).Split(',')
                                            .Select(m => m.Split('='))
                                            .ToDictionary(m => m[0], m => m[1]);
                        Import(fileName, mappings, delete, conn);
                        break;
                }
            });
        }
    }
}
