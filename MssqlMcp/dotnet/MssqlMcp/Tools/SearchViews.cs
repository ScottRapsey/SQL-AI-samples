// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.ComponentModel;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;

namespace Mssql.McpServer;

public partial class Tools
{
    private const string SearchViewsQuery = @"
        SELECT DISTINCT s.name, v.name 
        FROM sys.views v
        INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
        INNER JOIN sys.sql_modules m ON v.object_id = m.object_id
        WHERE m.definition LIKE @SearchTerm
        ORDER BY s.name, v.name";

    [McpServerTool(
        Title = "Search Views",
        ReadOnly = true,
        Idempotent = true,
        Destructive = false),
        Description("Searches for views containing a specific string in their definition.")]
    public async Task<DbOperationResult> SearchViews(
        [Description("The string to search for in the view definitions.")] string searchString,
        [Description("Optional database name. If not specified, uses the default database from connection string.")] string? database = null)
    {
        var conn = database == null
            ? await _connectionFactory.GetOpenConnectionAsync()
            : await _connectionFactory.GetOpenConnectionAsync(database);

        try
        {
            using (conn)
            {
                using var cmd = new SqlCommand(SearchViewsQuery, conn);
                cmd.Parameters.AddWithValue("@SearchTerm", $"%{searchString}%");

                var views = new List<string>();
                using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    views.Add($"{reader.GetString(0)}.{reader.GetString(1)}");
                }
                return new DbOperationResult(success: true, data: views);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SearchViews failed: {Message}", ex.Message);
            return new DbOperationResult(success: false, error: ex.Message);
        }
    }
}
