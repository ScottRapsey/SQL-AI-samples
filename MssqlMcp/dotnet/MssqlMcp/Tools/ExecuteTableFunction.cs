// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.ComponentModel;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;

namespace Mssql.McpServer;

public partial class Tools
{
    [McpServerTool(
        Title = "Execute Table Function",
        ReadOnly = true,
        Idempotent = true,
        Destructive = false),
        Description("Executes a table-valued function and returns the result set")]
    public async Task<DbOperationResult> ExecuteTableFunction(
        [Description("Name of table-valued function (can include schema, e.g., 'dbo.MyTableFunction')")] string name,
        [Description("Optional comma-separated parameter values, e.g., \"'value1', 123, '2024-01-01'\"")] string? parameters = null,
        [Description("Optional database name. If not specified, uses the default database from connection string.")] string? database = null)
    {
        string? schema = null;
        if (name.Contains('.'))
        {
            var parts = name.Split('.');
            if (parts.Length > 1)
            {
                schema = parts[0];
                name = parts[1];
            }
        }

        var funcName = schema != null ? $"[{schema}].[{name}]" : $"[{name}]";
        var paramList = string.IsNullOrEmpty(parameters) ? "" : parameters;
        var sql = $"SELECT * FROM {funcName}({paramList})";

        var conn = database == null
            ? await _connectionFactory.GetOpenConnectionAsync()
            : await _connectionFactory.GetOpenConnectionAsync(database);

        try
        {
            using (conn)
            {
                using var cmd = new SqlCommand(sql, conn);
                using var reader = await cmd.ExecuteReaderAsync();

                var results = new List<Dictionary<string, object?>>();

                while (await reader.ReadAsync())
                {
                    var row = new Dictionary<string, object?>();
                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        var value = reader.GetValue(i);
                        row[reader.GetName(i)] = value is DBNull ? null : value;
                    }
                    results.Add(row);
                }

                return new DbOperationResult(success: true, data: results);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ExecuteTableFunction failed: {Message}", ex.Message);
            return new DbOperationResult(success: false, error: ex.Message);
        }
    }
}