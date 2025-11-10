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
        Title = "Update Data",
        ReadOnly = false,
        Idempotent = false,
        Destructive = false),
        Description("Updates data in a table using an UPDATE statement")]
    public async Task<DbOperationResult> UpdateData(
        [Description("SQL UPDATE statement")] string sql,
        [Description("Optional database name. If not specified, uses the default database from connection string.")] string? database = null)
    {
        var conn = database == null 
            ? await _connectionFactory.GetOpenConnectionAsync()
            : await _connectionFactory.GetOpenConnectionAsync(database);
        
        try
        {
            using (conn)
            {
                using var cmd = new SqlCommand(sql, conn);
                var rowsAffected = await cmd.ExecuteNonQueryAsync();
                return new DbOperationResult(success: true, rowsAffected: rowsAffected);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "UpdateData failed: {Message}", ex.Message);
            return new DbOperationResult(success: false, error: ex.Message);
        }
    }
}

