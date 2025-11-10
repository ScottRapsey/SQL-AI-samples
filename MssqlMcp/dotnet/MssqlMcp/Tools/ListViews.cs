// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.ComponentModel;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;

namespace Mssql.McpServer;

public partial class Tools
{
    private const string ListViewsQuery = @"SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS ORDER BY TABLE_SCHEMA, TABLE_NAME";

    [McpServerTool(
        Title = "List Views",
        ReadOnly = true,
        Idempotent = true,
        Destructive = false),
        Description("Lists all views in the SQL Database.")]
    public async Task<DbOperationResult> ListViews(
        [Description("Optional database name. If not specified, uses the default database from connection string.")] string? database = null)
    {
        var conn = database == null
            ? await _connectionFactory.GetOpenConnectionAsync()
            : await _connectionFactory.GetOpenConnectionAsync(database);

        try
        {
            using (conn)
            {
                using var cmd = new SqlCommand(ListViewsQuery, conn);
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
            _logger.LogError(ex, "ListViews failed: {Message}", ex.Message);
            return new DbOperationResult(success: false, error: ex.Message);
        }
    }
}