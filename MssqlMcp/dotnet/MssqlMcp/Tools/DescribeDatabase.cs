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
        Title = "Describe Database",
        ReadOnly = true,
        Idempotent = true,
        Destructive = false),
        Description("Returns comprehensive metadata about a database including properties, size, file information, and object counts")]
    public async Task<DbOperationResult> DescribeDatabase(
        [Description("Optional database name. If not specified, describes the default database from connection string.")] string? database = null)
    {
        // Query for database properties - more version-safe query
        const string DatabaseInfoQuery = @"
            SELECT 
                db.name,
                db.database_id,
                db.create_date,
                db.compatibility_level,
                db.collation_name,
                db.user_access_desc,
                db.is_read_only,
                db.is_auto_close_on,
                db.is_auto_shrink_on,
                db.state_desc,
                db.recovery_model_desc,
                SUSER_SNAME(db.owner_sid) AS owner
            FROM sys.databases db
            WHERE db.name = DB_NAME()";

        // Query for optional columns (SQL Server 2008+)
        const string EncryptionQuery = @"
            SELECT 
                CASE 
                    WHEN EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('sys.databases') AND name = 'is_encrypted')
                    THEN (SELECT CAST(is_encrypted AS INT) FROM sys.databases WHERE database_id = DB_ID())
                    ELSE 0 
                END AS is_encrypted";

        // Query for change tracking (SQL Server 2016+)
        const string ChangeTrackingQuery = @"
            SELECT 
                CASE 
                    WHEN EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('sys.databases') AND name = 'is_change_tracking_enabled')
                    THEN (SELECT CAST(is_change_tracking_enabled AS INT) FROM sys.databases WHERE database_id = DB_ID())
                    ELSE 0 
                END AS is_change_tracking_enabled";

        // Query for database size
        const string DatabaseSizeQuery = @"
            SELECT 
                SUM(CAST(size AS BIGINT) * 8 / 1024) AS total_size_mb,
                SUM(CASE WHEN type = 0 THEN CAST(size AS BIGINT) * 8 / 1024 ELSE 0 END) AS data_size_mb,
                SUM(CASE WHEN type = 1 THEN CAST(size AS BIGINT) * 8 / 1024 ELSE 0 END) AS log_size_mb
            FROM sys.master_files
            WHERE database_id = DB_ID()";

        // Query for file information
        const string FilesQuery = @"
            SELECT 
                name AS file_name,
                physical_name,
                type_desc AS file_type,
                CAST(size AS BIGINT) * 8 / 1024 AS size_mb,
                CASE 
                    WHEN max_size = -1 THEN 'Unlimited'
                    WHEN max_size = 0 THEN 'No Growth'
                    ELSE CAST(CAST(max_size AS BIGINT) * 8 / 1024 AS VARCHAR) + ' MB'
                END AS max_size,
                CASE 
                    WHEN is_percent_growth = 1 THEN CAST(growth AS VARCHAR) + '%'
                    ELSE CAST(CAST(growth AS BIGINT) * 8 / 1024 AS VARCHAR) + ' MB'
                END AS growth_setting,
                state_desc
            FROM sys.database_files
            ORDER BY type, file_id";

        // Query for object counts
        const string ObjectCountsQuery = @"
            SELECT 
                (SELECT COUNT(*) FROM sys.tables WHERE is_ms_shipped = 0) AS table_count,
                (SELECT COUNT(*) FROM sys.views WHERE is_ms_shipped = 0) AS view_count,
                (SELECT COUNT(*) FROM sys.procedures WHERE is_ms_shipped = 0) AS procedure_count,
                (SELECT COUNT(*) FROM sys.objects WHERE type IN ('FN', 'IF', 'TF') AND is_ms_shipped = 0) AS function_count,
                (SELECT COUNT(*) FROM sys.triggers WHERE is_ms_shipped = 0) AS trigger_count,
                (SELECT COUNT(DISTINCT name) FROM sys.schemas WHERE schema_id > 4) AS user_schema_count";

        // Query for schemas
        const string SchemasQuery = @"
            SELECT 
                s.name AS schema_name,
                USER_NAME(s.principal_id) AS owner,
                s.schema_id
            FROM sys.schemas s
            WHERE s.schema_id > 4
            ORDER BY s.name";

        var conn = database == null
            ? await _connectionFactory.GetOpenConnectionAsync()
            : await _connectionFactory.GetOpenConnectionAsync(database);

        try
        {
            using (conn)
            {
                var result = new Dictionary<string, object>();

                // Database info (core properties that exist in all versions)
                using (var cmd = new SqlCommand(DatabaseInfoQuery, conn))
                {
                    using var reader = await cmd.ExecuteReaderAsync();
                    if (await reader.ReadAsync())
                    {
                        var dbInfo = new Dictionary<string, object?>
                        {
                            ["name"] = reader["name"],
                            ["database_id"] = reader["database_id"],
                            ["create_date"] = reader["create_date"],
                            ["compatibility_level"] = reader["compatibility_level"],
                            ["collation"] = reader["collation_name"],
                            ["user_access"] = reader["user_access_desc"],
                            ["is_read_only"] = (bool)reader["is_read_only"],
                            ["is_auto_close_on"] = (bool)reader["is_auto_close_on"],
                            ["is_auto_shrink_on"] = (bool)reader["is_auto_shrink_on"],
                            ["state"] = reader["state_desc"],
                            ["recovery_model"] = reader["recovery_model_desc"],
                            ["owner"] = reader["owner"]
                        };

                        // Try to get encryption status
                        try
                        {
                            using var encCmd = new SqlCommand(EncryptionQuery, conn);
                            var encResult = await encCmd.ExecuteScalarAsync();
                            dbInfo["is_encrypted"] = Convert.ToBoolean(encResult);
                        }
                        catch
                        {
                            dbInfo["is_encrypted"] = false;
                        }

                        // Try to get change tracking status
                        try
                        {
                            using var ctCmd = new SqlCommand(ChangeTrackingQuery, conn);
                            var ctResult = await ctCmd.ExecuteScalarAsync();
                            dbInfo["is_change_tracking_enabled"] = Convert.ToBoolean(ctResult);
                        }
                        catch
                        {
                            dbInfo["is_change_tracking_enabled"] = false;
                        }

                        result["database"] = dbInfo;
                    }
                }

                // Database size
                using (var cmd = new SqlCommand(DatabaseSizeQuery, conn))
                {
                    using var reader = await cmd.ExecuteReaderAsync();
                    if (await reader.ReadAsync())
                    {
                        result["size"] = new
                        {
                            total_mb = reader["total_size_mb"],
                            data_mb = reader["data_size_mb"],
                            log_mb = reader["log_size_mb"]
                        };
                    }
                }

                // Files
                using (var cmd = new SqlCommand(FilesQuery, conn))
                {
                    using var reader = await cmd.ExecuteReaderAsync();
                    var files = new List<object>();
                    while (await reader.ReadAsync())
                    {
                        files.Add(new
                        {
                            file_name = reader["file_name"],
                            physical_name = reader["physical_name"],
                            file_type = reader["file_type"],
                            size_mb = reader["size_mb"],
                            max_size = reader["max_size"],
                            growth_setting = reader["growth_setting"],
                            state = reader["state_desc"]
                        });
                    }
                    result["files"] = files;
                }

                // Object counts
                using (var cmd = new SqlCommand(ObjectCountsQuery, conn))
                {
                    using var reader = await cmd.ExecuteReaderAsync();
                    if (await reader.ReadAsync())
                    {
                        result["object_counts"] = new
                        {
                            tables = reader["table_count"],
                            views = reader["view_count"],
                            stored_procedures = reader["procedure_count"],
                            functions = reader["function_count"],
                            triggers = reader["trigger_count"],
                            user_schemas = reader["user_schema_count"]
                        };
                    }
                }

                // Schemas
                using (var cmd = new SqlCommand(SchemasQuery, conn))
                {
                    using var reader = await cmd.ExecuteReaderAsync();
                    var schemas = new List<object>();
                    while (await reader.ReadAsync())
                    {
                        schemas.Add(new
                        {
                            name = reader["schema_name"],
                            owner = reader["owner"],
                            schema_id = reader["schema_id"]
                        });
                    }
                    result["schemas"] = schemas;
                }

                return new DbOperationResult(success: true, data: result);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "DescribeDatabase failed: {Message}", ex.Message);
            return new DbOperationResult(success: false, error: ex.Message);
        }
    }
}