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
        Title = "Describe Stored Procedure",
        ReadOnly = true,
        Idempotent = true,
        Destructive = false),
        Description("Returns stored procedure metadata including parameters and definition")]
    public async Task<DbOperationResult> DescribeStoredProcedure(
        [Description("Name of stored procedure")] string name,
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

        // Query for stored procedure metadata
        const string ProcedureInfoQuery = @"SELECT p.object_id AS id, p.name, s.name AS [schema], ep.value AS description, p.type, u.name AS owner, p.create_date, p.modify_date
            FROM sys.procedures p
            INNER JOIN sys.schemas s ON p.schema_id = s.schema_id
            LEFT JOIN sys.extended_properties ep ON ep.major_id = p.object_id AND ep.minor_id = 0 AND ep.name = 'MS_Description'
            LEFT JOIN sys.sysusers u ON p.principal_id = u.uid
            WHERE p.name = @ProcedureName AND (s.name = @ProcedureSchema OR @ProcedureSchema IS NULL)";

        // Query for parameters
        const string ParametersQuery = @"SELECT 
                param.name,
                TYPE_NAME(param.user_type_id) AS type,
                param.max_length AS length,
                param.precision,
                param.scale,
                param.is_output,
                param.has_default_value,
                param.default_value
            FROM sys.parameters param
            WHERE param.object_id = (SELECT object_id FROM sys.procedures p INNER JOIN sys.schemas s ON p.schema_id = s.schema_id WHERE p.name = @ProcedureName AND (s.name = @ProcedureSchema OR @ProcedureSchema IS NULL))
            ORDER BY param.parameter_id";

        // Query for procedure definition
        const string DefinitionQuery = @"SELECT m.definition
            FROM sys.sql_modules m
            INNER JOIN sys.procedures p ON m.object_id = p.object_id
            INNER JOIN sys.schemas s ON p.schema_id = s.schema_id
            WHERE p.name = @ProcedureName AND (s.name = @ProcedureSchema OR @ProcedureSchema IS NULL)";

        // Query for dependencies
        const string DependenciesQuery = @"SELECT DISTINCT 
                SCHEMA_NAME(o.schema_id) AS referenced_schema,
                o.name AS referenced_object,
                o.type_desc AS object_type
            FROM sys.sql_expression_dependencies d
            INNER JOIN sys.objects o ON d.referenced_id = o.object_id
            WHERE d.referencing_id = (SELECT object_id FROM sys.procedures p INNER JOIN sys.schemas s ON p.schema_id = s.schema_id WHERE p.name = @ProcedureName AND (s.name = @ProcedureSchema OR @ProcedureSchema IS NULL))";

        var conn = database == null 
            ? await _connectionFactory.GetOpenConnectionAsync()
            : await _connectionFactory.GetOpenConnectionAsync(database);
        
        try
        {
            using (conn)
            {
                var result = new Dictionary<string, object>();
                
                // Procedure info
                using (var cmd = new SqlCommand(ProcedureInfoQuery, conn))
                {
                    cmd.Parameters.AddWithValue("@ProcedureName", name);
                    cmd.Parameters.AddWithValue("@ProcedureSchema", schema == null ? DBNull.Value : schema);
                    using var reader = await cmd.ExecuteReaderAsync();
                    if (await reader.ReadAsync())
                    {
                        result["procedure"] = new
                        {
                            id = reader["id"],
                            name = reader["name"],
                            schema = reader["schema"],
                            owner = reader["owner"],
                            type = reader["type"],
                            create_date = reader["create_date"],
                            modify_date = reader["modify_date"],
                            description = reader["description"] is DBNull ? null : reader["description"]
                        };
                    }
                    else
                    {
                        return new DbOperationResult(success: false, error: $"Stored procedure '{name}' not found.");
                    }
                }

                // Parameters
                using (var cmd = new SqlCommand(ParametersQuery, conn))
                {
                    cmd.Parameters.AddWithValue("@ProcedureName", name);
                    cmd.Parameters.AddWithValue("@ProcedureSchema", schema == null ? DBNull.Value : schema);
                    using var reader = await cmd.ExecuteReaderAsync();
                    var parameters = new List<object>();
                    while (await reader.ReadAsync())
                    {
                        parameters.Add(new
                        {
                            name = reader["name"],
                            type = reader["type"],
                            length = reader["length"],
                            precision = reader["precision"],
                            scale = reader["scale"],
                            is_output = (bool)reader["is_output"],
                            has_default_value = (bool)reader["has_default_value"],
                            default_value = reader["default_value"] is DBNull ? null : reader["default_value"]
                        });
                    }
                    result["parameters"] = parameters;
                }

                // Definition
                using (var cmd = new SqlCommand(DefinitionQuery, conn))
                {
                    cmd.Parameters.AddWithValue("@ProcedureName", name);
                    cmd.Parameters.AddWithValue("@ProcedureSchema", schema == null ? DBNull.Value : schema);
                    using var reader = await cmd.ExecuteReaderAsync();
                    if (await reader.ReadAsync())
                    {
                        result["definition"] = reader["definition"] is DBNull ? null : reader["definition"];
                    }
                }

                // Dependencies
                using (var cmd = new SqlCommand(DependenciesQuery, conn))
                {
                    cmd.Parameters.AddWithValue("@ProcedureName", name);
                    cmd.Parameters.AddWithValue("@ProcedureSchema", schema == null ? DBNull.Value : schema);
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
            _logger.LogError(ex, "DescribeStoredProcedure failed: {Message}", ex.Message);
            return new DbOperationResult(success: false, error: ex.Message);
        }
    }
}