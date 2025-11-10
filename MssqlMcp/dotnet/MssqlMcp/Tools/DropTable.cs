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
        Title = "Drop Table",
        ReadOnly = false,
        Idempotent = true,
        Destructive = true),
        Description("Drops a table from the SQL Database")]
    public async Task<DbOperationResult> DropTable(
        [Description("Name of table to drop")] string name,
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

        var tableName = schema != null ? $"[{schema}].[{name}]" : $"[{name}]";
        var sql = $"DROP TABLE IF EXISTS {tableName}";

        var conn = database == null
            ? await _connectionFactory.GetOpenConnectionAsync()
            : await _connectionFactory.GetOpenConnectionAsync(database);

        try
        {
            using (conn)
            {
                using var cmd = new SqlCommand(sql, conn);
                _ = await cmd.ExecuteNonQueryAsync();
                return new DbOperationResult(success: true, rowsAffected: 0);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "DropTable failed: {Message}", ex.Message);
            return new DbOperationResult(success: false, error: ex.Message);
        }
    }
}
