// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.ComponentModel;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;

namespace Mssql.McpServer;

public partial class Tools
{
    private const string ListDatabasesQuery = @"
        SELECT 
            name,
            database_id,
            create_date,
            state_desc AS state,
            recovery_model_desc AS recovery_model,
            compatibility_level
        FROM sys.databases
        WHERE state_desc = 'ONLINE'
        ORDER BY name";

    [McpServerTool(
        Title = "List Databases",
        ReadOnly = true,
        Idempotent = true,
        Destructive = false),
        Description("Lists all online databases on the SQL Server instance.")]
    public async Task<DbOperationResult> ListDatabases()
    {
        // Use the default connection - sys.databases is accessible from any database
        var conn = await _connectionFactory.GetOpenConnectionAsync();
        try
        {
            using (conn)
            {
                using var cmd = new SqlCommand(ListDatabasesQuery, conn);
                var databases = new List<object>();
                using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    databases.Add(new
                    {
                        name = reader.GetString(0),
                        database_id = reader.GetInt32(1),
                        create_date = reader.GetDateTime(2),
                        state = reader.GetString(3),
                        recovery_model = reader.GetString(4),
                        compatibility_level = reader.GetByte(5)
                    });
                }
                return new DbOperationResult(success: true, data: databases);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ListDatabases failed: {Message}", ex.Message);
            return new DbOperationResult(success: false, error: ex.Message);
        }
    }
}