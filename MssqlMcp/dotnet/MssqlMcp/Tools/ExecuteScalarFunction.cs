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
        Title = "Execute Scalar Function",
        ReadOnly = true,
        Idempotent = true,
        Destructive = false),
        Description("Executes a scalar function and returns the result value")]
    public async Task<DbOperationResult> ExecuteScalarFunction(
        [Description("Name of scalar function (can include schema, e.g., 'dbo.MyFunction')")] string name,
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
        var sql = $"SELECT {funcName}({paramList}) AS Result";

        var conn = database == null
            ? await _connectionFactory.GetOpenConnectionAsync()
            : await _connectionFactory.GetOpenConnectionAsync(database);

        try
        {
            using (conn)
            {
                using var cmd = new SqlCommand(sql, conn);
                var result = await cmd.ExecuteScalarAsync();

                return new DbOperationResult(success: true, data: new
                {
                    result = result is DBNull ? null : result
                });
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ExecuteScalarFunction failed: {Message}", ex.Message);
            return new DbOperationResult(success: false, error: ex.Message);
        }
    }
}