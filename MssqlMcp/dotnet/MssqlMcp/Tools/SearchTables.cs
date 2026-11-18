// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.ComponentModel;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;

namespace Mssql.McpServer;

public partial class Tools
{
    private const string SearchTablesQuery = @"
        SELECT DISTINCT s.name, t.name 
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        LEFT JOIN sys.columns c ON t.object_id = c.object_id
        WHERE t.name LIKE @SearchTerm OR c.name LIKE @SearchTerm
        ORDER BY s.name, t.name";

    [McpServerTool(
        Title = "Search Tables",
        ReadOnly = true,
        Idempotent = true,
        Destructive = false),
        Description("Searches for tables where the table name or any column name contains the search string.")]
    public async Task<DbOperationResult> SearchTables(
        [Description("The string to search for in table names or column names.")] string searchString,
        [Description("Optional database name. If not specified, uses the default database from connection string.")] string? database = null)
    {
        var conn = database == null
            ? await _connectionFactory.GetOpenConnectionAsync()
            : await _connectionFactory.GetOpenConnectionAsync(database);

        try
        {
            using (conn)
            {
                using var cmd = new SqlCommand(SearchTablesQuery, conn);
                cmd.Parameters.AddWithValue("@SearchTerm", $"%{searchString}%");

                var tables = new List<string>();
                using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    tables.Add($"{reader.GetString(0)}.{reader.GetString(1)}");
                }
                return new DbOperationResult(success: true, data: tables);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SearchTables failed: {Message}", ex.Message);
            return new DbOperationResult(success: false, error: ex.Message);
        }
    }
}
