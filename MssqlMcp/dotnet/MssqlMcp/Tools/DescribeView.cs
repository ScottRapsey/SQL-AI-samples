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
        Title = "Describe View",
        ReadOnly = true,
        Idempotent = true,
        Destructive = false),
        Description("Returns view schema including columns, indexes, and definition")]
    public async Task<DbOperationResult> DescribeView(
        [Description("Name of view")] string name,
        [Description("Optional database name. If not specified, uses the default database from connection string.")] string? database = null)
    {
        string? schema = null;
        if (name.Contains('.'))
        {
            var parts = name.Split('.');
            if (parts.Length > 1)
            {
                name = parts[1];
                schema = parts[0];
            }
        }

        // Query for view metadata
        const string ViewInfoQuery = @"SELECT v.object_id AS id, v.name, s.name AS [schema], p.value AS description, v.type, u.name AS owner
            FROM sys.views v
            INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
            LEFT JOIN sys.extended_properties p ON p.major_id = v.object_id AND p.minor_id = 0 AND p.name = 'MS_Description'
            LEFT JOIN sys.sysusers u ON v.principal_id = u.uid
            WHERE v.name = @ViewName AND (s.name = @ViewSchema OR @ViewSchema IS NULL)";

        // Query for columns
        const string ColumnsQuery = @"SELECT c.name, ty.name AS type, c.max_length AS length, c.precision, c.scale, c.is_nullable AS nullable, p.value AS description
            FROM sys.columns c
            INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
            LEFT JOIN sys.extended_properties p ON p.major_id = c.object_id AND p.minor_id = c.column_id AND p.name = 'MS_Description'
            WHERE c.object_id = (SELECT object_id FROM sys.views v INNER JOIN sys.schemas s ON v.schema_id = s.schema_id WHERE v.name = @ViewName AND (s.name = @ViewSchema OR @ViewSchema IS NULL))";

        // Query for indexes (for indexed views)
        const string IndexesQuery = @"SELECT i.name, i.type_desc AS type, p.value AS description,
            STUFF((SELECT ',' + c.name FROM sys.index_columns ic
                INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id ORDER BY ic.key_ordinal FOR XML PATH('')), 1, 1, '') AS keys
            FROM sys.indexes i
            LEFT JOIN sys.extended_properties p ON p.major_id = i.object_id AND p.minor_id = i.index_id AND p.name = 'MS_Description'
            WHERE i.object_id = (SELECT object_id FROM sys.views v INNER JOIN sys.schemas s ON v.schema_id = s.schema_id WHERE v.name = @ViewName AND (s.name = @ViewSchema OR @ViewSchema IS NULL))";

        // Query for view definition
        const string DefinitionQuery = @"SELECT m.definition
            FROM sys.sql_modules m
            INNER JOIN sys.views v ON m.object_id = v.object_id
            INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
            WHERE v.name = @ViewName AND (s.name = @ViewSchema OR @ViewSchema IS NULL)";

        // Query for dependencies
        const string DependenciesQuery = @"SELECT DISTINCT 
                SCHEMA_NAME(o.schema_id) AS referenced_schema,
                o.name AS referenced_object,
                o.type_desc AS object_type
            FROM sys.sql_expression_dependencies d
            INNER JOIN sys.objects o ON d.referenced_id = o.object_id
            WHERE d.referencing_id = (SELECT object_id FROM sys.views v INNER JOIN sys.schemas s ON v.schema_id = s.schema_id WHERE v.name = @ViewName AND (s.name = @ViewSchema OR @ViewSchema IS NULL))";

        var conn = database == null 
            ? await _connectionFactory.GetOpenConnectionAsync()
            : await _connectionFactory.GetOpenConnectionAsync(database);
        
        try
        {
            using (conn)
            {
                var result = new Dictionary<string, object>();
                
                // View info
                using (var cmd = new SqlCommand(ViewInfoQuery, conn))
                {
                    cmd.Parameters.AddWithValue("@ViewName", name);
                    cmd.Parameters.AddWithValue("@ViewSchema", schema == null ? DBNull.Value : schema);
                    using var reader = await cmd.ExecuteReaderAsync();
                    if (await reader.ReadAsync())
                    {
                        result["view"] = new
                        {
                            id = reader["id"],
                            name = reader["name"],
                            schema = reader["schema"],
                            owner = reader["owner"],
                            type = reader["type"],
                            description = reader["description"] is DBNull ? null : reader["description"]
                        };
                    }
                    else
                    {
                        return new DbOperationResult(success: false, error: $"View '{name}' not found.");
                    }
                }

                // Columns
                using (var cmd = new SqlCommand(ColumnsQuery, conn))
                {
                    cmd.Parameters.AddWithValue("@ViewName", name);
                    cmd.Parameters.AddWithValue("@ViewSchema", schema == null ? DBNull.Value : schema);
                    using var reader = await cmd.ExecuteReaderAsync();
                    var columns = new List<object>();
                    while (await reader.ReadAsync())
                    {
                        columns.Add(new
                        {
                            name = reader["name"],
                            type = reader["type"],
                            length = reader["length"],
                            precision = reader["precision"],
                            scale = reader["scale"],
                            nullable = (bool)reader["nullable"],
                            description = reader["description"] is DBNull ? null : reader["description"]
                        });
                    }
                    result["columns"] = columns;
                }

                // Indexes
                using (var cmd = new SqlCommand(IndexesQuery, conn))
                {
                    cmd.Parameters.AddWithValue("@ViewName", name);
                    cmd.Parameters.AddWithValue("@ViewSchema", schema == null ? DBNull.Value : schema);
                    using var reader = await cmd.ExecuteReaderAsync();
                    var indexes = new List<object>();
                    while (await reader.ReadAsync())
                    {
                        indexes.Add(new
                        {
                            name = reader["name"],
                            type = reader["type"],
                            description = reader["description"] is DBNull ? null : reader["description"],
                            keys = reader["keys"]
                        });
                    }
                    result["indexes"] = indexes;
                }

                // Definition
                using (var cmd = new SqlCommand(DefinitionQuery, conn))
                {
                    cmd.Parameters.AddWithValue("@ViewName", name);
                    cmd.Parameters.AddWithValue("@ViewSchema", schema == null ? DBNull.Value : schema);
                    using var reader = await cmd.ExecuteReaderAsync();
                    if (await reader.ReadAsync())
                    {
                        result["definition"] = reader["definition"] is DBNull ? null : reader["definition"];
                    }
                }

                // Dependencies
                using (var cmd = new SqlCommand(DependenciesQuery, conn))
                {
                    cmd.Parameters.AddWithValue("@ViewName", name);
                    cmd.Parameters.AddWithValue("@ViewSchema", schema == null ? DBNull.Value : schema);
                    using var reader = await cmd.ExecuteReaderAsync();
                    var dependencies = new List<object>();
                    while (await reader.ReadAsync())
                    {
                        dependencies.Add(new
                        {
                            referenced_schema = reader["referenced_schema"],
                            referenced_object = reader["referenced_object"],
                            object_type = reader["object_type"]
                        });
                    }
                    result["dependencies"] = dependencies;
                }

                return new DbOperationResult(success: true, data: result);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "DescribeView failed: {Message}", ex.Message);
            return new DbOperationResult(success: false, error: ex.Message);
        }
    }
}