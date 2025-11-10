// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.ComponentModel;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;

namespace Mssql.McpServer;

public partial class Tools
{
    private const string ListStoredProceduresQuery = @"SELECT ROUTINE_SCHEMA, ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME";

    [McpServerTool(
        Title = "List Stored Procedures",
        ReadOnly = true,
        Idempotent = true,
        Destructive = false),
        Description("Lists all stored procedures in the SQL Database.")]
    public async Task<DbOperationResult> ListStoredProcedures(
        [Description("Optional database name. If not specified, uses the default database from connection string.")] string? database = null)
    {
        var conn = database == null
            ? await _connectionFactory.GetOpenConnectionAsync()
            : await _connectionFactory.GetOpenConnectionAsync(database);

        try
        {
            using (conn)
            {
                using var cmd = new SqlCommand(ListStoredProceduresQuery, conn);
                var procedures = new List<string>();
                using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    procedures.Add($"{reader.GetString(0)}.{reader.GetString(1)}");
                }
                return new DbOperationResult(success: true, data: procedures);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ListStoredProcedures failed: {Message}", ex.Message);
            return new DbOperationResult(success: false, error: ex.Message);
        }
    }
}