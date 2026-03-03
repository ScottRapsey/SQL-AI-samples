// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.ComponentModel;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;

namespace Mssql.McpServer;

public partial class Tools
{
    private const string SearchFunctionsQuery = @"
        SELECT DISTINCT s.name, o.name 
        FROM sys.objects o
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        INNER JOIN sys.sql_modules m ON o.object_id = m.object_id
        WHERE o.type IN ('FN', 'IF', 'TF')
        AND m.definition LIKE @SearchTerm
        ORDER BY s.name, o.name";

    [McpServerTool(
        Title = "Search Functions",
        ReadOnly = true,
        Idempotent = true,
        Destructive = false),
        Description("Searches for functions containing a specific string in their definition.")]
    public async Task<DbOperationResult> SearchFunctions(
        [Description("The string to search for in the function definitions.")] string searchString,
        [Description("Optional database name. If not specified, uses the default database from connection string.")] string? database = null)
    {
        var conn = database == null
            ? await _connectionFactory.GetOpenConnectionAsync()
            : await _connectionFactory.GetOpenConnectionAsync(database);

        try
        {
            using (conn)
            {
                using var cmd = new SqlCommand(SearchFunctionsQuery, conn);
                cmd.Parameters.AddWithValue("@SearchTerm", $"%{searchString}%");

                var functions = new List<string>();
                using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    functions.Add($"{reader.GetString(0)}.{reader.GetString(1)}");
                }
                return new DbOperationResult(success: true, data: functions);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SearchFunctions failed: {Message}", ex.Message);
            return new DbOperationResult(success: false, error: ex.Message);
        }
    }
}
