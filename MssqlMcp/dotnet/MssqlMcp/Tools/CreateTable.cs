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
        Title = "Create Table",
        ReadOnly = false,
        Idempotent = false,
        Destructive = false),
        Description("Creates a new table in the SQL Database")]
    public async Task<DbOperationResult> CreateTable(
        [Description("SQL CREATE TABLE statement")] string sql,
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
                await cmd.ExecuteNonQueryAsync();
                return new DbOperationResult(success: true, rowsAffected: 0);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "CreateTable failed: {Message}", ex.Message);
            return new DbOperationResult(success: false, error: ex.Message);
        }
    }
}
