// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Data;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;

namespace Mssql.McpServer;

// Register this class as a tool container
[McpServerToolType]
public partial class Tools(ISqlConnectionFactory connectionFactory, ILogger<Tools> logger)
{
    private readonly ISqlConnectionFactory _connectionFactory = connectionFactory;
    private readonly ILogger<Tools> _logger = logger;

    // Helper to convert DataTable to a serializable list
    private static List<Dictionary<string, object>> DataTableToList(DataTable table)
    {
        var result = new List<Dictionary<string, object>>();
        foreach (DataRow row in table.Rows)
        {
            var dict = new Dictionary<string, object>();
            foreach (DataColumn col in table.Columns)
            {
                dict[col.ColumnName] = row[col];
            }
            result.Add(dict);
        }
        return result;
    }

    private static string GetSqlTypeForValue(object? value)
    {
        return value switch
        {
            null => "SQL_VARIANT",
            int => "INT",
            long => "BIGINT",
            decimal => "DECIMAL(38,10)",
            double or float => "FLOAT",
            bool => "BIT",
            string s => $"NVARCHAR({Math.Max(s.Length * 2, 50)})",
            DateTime => "DATETIME2",
            _ => "SQL_VARIANT"
        };
    }

    private static object? ConvertJsonElementToObject(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.TryGetInt32(out var intValue) ? intValue :
                                   element.TryGetInt64(out var longValue) ? longValue :
                                   element.TryGetDecimal(out var decimalValue) ? decimalValue :
                                   element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            JsonValueKind.Undefined => null,
            _ => element.ToString()
        };
    }
}