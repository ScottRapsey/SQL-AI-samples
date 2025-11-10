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
        Title = "Describe Instance",
        ReadOnly = true,
        Idempotent = true,
        Destructive = false),
        Description("Returns SQL Server instance information including version, edition, server properties, and configuration")]
    public async Task<DbOperationResult> DescribeInstance()
    {
        // Query for SQL Server version and edition
        const string VersionQuery = @"
            SELECT 
                SERVERPROPERTY('MachineName') AS machine_name,
                SERVERPROPERTY('ServerName') AS server_name,
                SERVERPROPERTY('InstanceName') AS instance_name,
                SERVERPROPERTY('ProductVersion') AS product_version,
                SERVERPROPERTY('ProductLevel') AS product_level,
                SERVERPROPERTY('Edition') AS edition,
                SERVERPROPERTY('EngineEdition') AS engine_edition,
                SERVERPROPERTY('Collation') AS collation,
                SERVERPROPERTY('IsIntegratedSecurityOnly') AS is_windows_auth_only,
                SERVERPROPERTY('IsClustered') AS is_clustered,
                SERVERPROPERTY('IsHadrEnabled') AS is_hadr_enabled,
                SERVERPROPERTY('IsFullTextInstalled') AS is_fulltext_installed,
                @@VERSION AS version_string";

        // Query for server configuration
        const string ConfigQuery = @"
            SELECT 
                (SELECT CAST(value_in_use AS INT) FROM sys.configurations WHERE name = 'max server memory (MB)') AS max_memory_mb,
                (SELECT CAST(value_in_use AS INT) FROM sys.configurations WHERE name = 'min server memory (MB)') AS min_memory_mb,
                (SELECT CAST(value_in_use AS INT) FROM sys.configurations WHERE name = 'max degree of parallelism') AS max_dop,
                (SELECT CAST(value_in_use AS INT) FROM sys.configurations WHERE name = 'cost threshold for parallelism') AS cost_threshold_parallelism";

        // Query for CPU and memory info
        const string ResourceQuery = @"
            SELECT 
                cpu_count AS logical_cpu_count,
                hyperthread_ratio,
                physical_memory_kb / 1024 AS physical_memory_mb,
                virtual_memory_kb / 1024 AS virtual_memory_mb,
                committed_kb / 1024 AS committed_memory_mb,
                committed_target_kb / 1024 AS committed_target_mb
            FROM sys.dm_os_sys_info";

        // Query for database count
        const string DatabaseCountQuery = @"
            SELECT 
                COUNT(*) AS total_databases,
                SUM(CASE WHEN state_desc = 'ONLINE' THEN 1 ELSE 0 END) AS online_databases,
                SUM(CASE WHEN name NOT IN ('master', 'tempdb', 'model', 'msdb') THEN 1 ELSE 0 END) AS user_databases
            FROM sys.databases";

        var conn = await _connectionFactory.GetOpenConnectionAsync();

        try
        {
            using (conn)
            {
                var result = new Dictionary<string, object>();

                // Version and edition info
                using (var cmd = new SqlCommand(VersionQuery, conn))
                {
                    using var reader = await cmd.ExecuteReaderAsync();
                    if (await reader.ReadAsync())
                    {
                        result["instance"] = new
                        {
                            machine_name = reader["machine_name"],
                            server_name = reader["server_name"],
                            instance_name = reader["instance_name"] is DBNull ? "Default" : reader["instance_name"],
                            product_version = reader["product_version"],
                            product_level = reader["product_level"],
                            edition = reader["edition"],
                            engine_edition = reader["engine_edition"],
                            collation = reader["collation"],
                            is_windows_auth_only = Convert.ToBoolean(reader["is_windows_auth_only"]),
                            is_clustered = Convert.ToBoolean(reader["is_clustered"]),
                            is_hadr_enabled = Convert.ToBoolean(reader["is_hadr_enabled"]),
                            is_fulltext_installed = Convert.ToBoolean(reader["is_fulltext_installed"]),
                            version_string = reader["version_string"]
                        };
                    }
                }

                // Configuration
                using (var cmd = new SqlCommand(ConfigQuery, conn))
                {
                    using var reader = await cmd.ExecuteReaderAsync();
                    if (await reader.ReadAsync())
                    {
                        result["configuration"] = new
                        {
                            max_server_memory_mb = reader["max_memory_mb"],
                            min_server_memory_mb = reader["min_memory_mb"],
                            max_degree_of_parallelism = reader["max_dop"],
                            cost_threshold_for_parallelism = reader["cost_threshold_parallelism"]
                        };
                    }
                }

                // Resource info
                using (var cmd = new SqlCommand(ResourceQuery, conn))
                {
                    using var reader = await cmd.ExecuteReaderAsync();
                    if (await reader.ReadAsync())
                    {
                        result["resources"] = new
                        {
                            logical_cpu_count = reader["logical_cpu_count"],
                            hyperthread_ratio = reader["hyperthread_ratio"],
                            physical_memory_mb = reader["physical_memory_mb"],
                            virtual_memory_mb = reader["virtual_memory_mb"],
                            committed_memory_mb = reader["committed_memory_mb"],
                            committed_target_mb = reader["committed_target_mb"]
                        };
                    }
                }

                // Database counts
                using (var cmd = new SqlCommand(DatabaseCountQuery, conn))
                {
                    using var reader = await cmd.ExecuteReaderAsync();
                    if (await reader.ReadAsync())
                    {
                        result["database_summary"] = new
                        {
                            total_databases = reader["total_databases"],
                            online_databases = reader["online_databases"],
                            user_databases = reader["user_databases"]
                        };
                    }
                }

                return new DbOperationResult(success: true, data: result);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "DescribeInstance failed: {Message}", ex.Message);
            return new DbOperationResult(success: false, error: ex.Message);
        }
    }
}