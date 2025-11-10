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
        Title = "Describe Function",
        ReadOnly = true,
        Idempotent = true,
        Destructive = false),
        Description("Returns function metadata including parameters, return type, and definition")]
    public async Task<DbOperationResult> DescribeFunction(
        [Description("Name of function")] string name,
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

        // Query for function metadata
        const string FunctionInfoQuery = @"SELECT 
                o.object_id AS id, 
                o.name, 
                s.name AS [schema], 
                ep.value AS description, 
                o.type, 
                o.type_desc,
                u.name AS owner, 
                o.create_date, 
                o.modify_date
            FROM sys.objects o
            INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
            LEFT JOIN sys.extended_properties ep ON ep.major_id = o.object_id AND ep.minor_id = 0 AND ep.name = 'MS_Description'
            LEFT JOIN sys.sysusers u ON o.principal_id = u.uid
            WHERE o.type IN ('FN', 'IF', 'TF') AND o.name = @FunctionName AND (s.name = @FunctionSchema OR @FunctionSchema IS NULL)";

        // Query for parameters (input parameters)
        const string ParametersQuery = @"SELECT 
                param.name,
                TYPE_NAME(param.user_type_id) AS type,
                param.max_length AS length,
                param.precision,
                param.scale,
                param.has_default_value,
                param.default_value
            FROM sys.parameters param
            WHERE param.object_id = (SELECT object_id FROM sys.objects o INNER JOIN sys.schemas s ON o.schema_id = s.schema_id WHERE o.type IN ('FN', 'IF', 'TF') AND o.name = @FunctionName AND (s.name = @FunctionSchema OR @FunctionSchema IS NULL))
            AND param.parameter_id > 0
            ORDER BY param.parameter_id";

        // Query for return type
        const string ReturnTypeQuery = @"SELECT 
                TYPE_NAME(param.user_type_id) AS return_type,
                param.max_length AS length,
                param.precision,
                param.scale
            FROM sys.parameters param
            WHERE param.object_id = (SELECT object_id FROM sys.objects o INNER JOIN sys.schemas s ON o.schema_id = s.schema_id WHERE o.type IN ('FN', 'IF', 'TF') AND o.name = @FunctionName AND (s.name = @FunctionSchema OR @FunctionSchema IS NULL))
            AND param.parameter_id = 0";

        // Query for table-valued function columns (if applicable)
        const string TableColumnsQuery = @"SELECT 
                c.name,
                TYPE_NAME(c.user_type_id) AS type,
                c.max_length AS length,
                c.precision,
                c.scale,
                c.is_nullable AS nullable
            FROM sys.columns c
            WHERE c.object_id = (SELECT object_id FROM sys.objects o INNER JOIN sys.schemas s ON o.schema_id = s.schema_id WHERE o.type IN ('IF', 'TF') AND o.name = @FunctionName AND (s.name = @FunctionSchema OR @FunctionSchema IS NULL))
            ORDER BY c.column_id";

        // Query for function definition
        const string DefinitionQuery = @"SELECT m.definition
            FROM sys.sql_modules m
            INNER JOIN sys.objects o ON m.object_id = o.object_id
            INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE o.type IN ('FN', 'IF', 'TF') AND o.name = @FunctionName AND (s.name = @FunctionSchema OR @FunctionSchema IS NULL)";

        // Query for dependencies
        const string DependenciesQuery = @"SELECT DISTINCT 
                SCHEMA_NAME(o.schema_id) AS referenced_schema,
                o.name AS referenced_object,
                o.type_desc AS object_type
            FROM sys.sql_expression_dependencies d
            INNER JOIN sys.objects o ON d.referenced_id = o.object_id
            WHERE d.referencing_id = (SELECT object_id FROM sys.objects obj INNER JOIN sys.schemas s ON obj.schema_id = s.schema_id WHERE obj.type IN ('FN', 'IF', 'TF') AND obj.name = @FunctionName AND (s.name = @FunctionSchema OR @FunctionSchema IS NULL))";

        var conn = database == null 
            ? await _connectionFactory.GetOpenConnectionAsync()
            : await _connectionFactory.GetOpenConnectionAsync(database);
        
        try
        {
            using (conn)
            {
                var result = new Dictionary<string, object>();
                string? functionType = null;
                
                // Function info
                using (var cmd = new SqlCommand(FunctionInfoQuery, conn))
                {
                    cmd.Parameters.AddWithValue("@FunctionName", name);
                    cmd.Parameters.AddWithValue("@FunctionSchema", schema == null ? DBNull.Value : schema);
                    using var reader = await cmd.ExecuteReaderAsync();
                    if (await reader.ReadAsync())
                    {
                        functionType = reader["type"].ToString();
                        result["function"] = new
                        {
                            id = reader["id"],
                            name = reader["name"],
                            schema = reader["schema"],
                            owner = reader["owner"],
                            type = reader["type"],
                            type_description = reader["type_desc"],
                            create_date = reader["create_date"],
                            modify_date = reader["modify_date"],
                            description = reader["description"] is DBNull ? null : reader["description"]
                        };
                    }
                    else
                    {
                        return new DbOperationResult(success: false, error: $"Function '{name}' not found.");
                    }
                }

                // Parameters
                using (var cmd = new SqlCommand(ParametersQuery, conn))
                {
                    cmd.Parameters.AddWithValue("@FunctionName", name);
                    cmd.Parameters.AddWithValue("@FunctionSchema", schema == null ? DBNull.Value : schema);
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
                            has_default_value = (bool)reader["has_default_value"],
                            default_value = reader["default_value"] is DBNull ? null : reader["default_value"]
                        });
                    }
                    result["parameters"] = parameters;
                }

                // Return type (for scalar functions)
                using (var cmd = new SqlCommand(ReturnTypeQuery, conn))
                {
                    cmd.Parameters.AddWithValue("@FunctionName", name);
                    cmd.Parameters.AddWithValue("@FunctionSchema", schema == null ? DBNull.Value : schema);
                    using var reader = await cmd.ExecuteReaderAsync();
                    if (await reader.ReadAsync())
                    {
                        result["return_type"] = new
                        {
                            type = reader["return_type"],
                            length = reader["length"],
                            precision = reader["precision"],
                            scale = reader["scale"]
                        };
                    }
                }

                // Table columns (for table-valued functions)
                if (functionType == "IF" || functionType == "TF")
                {
                    using (var cmd = new SqlCommand(TableColumnsQuery, conn))
                    {
                        cmd.Parameters.AddWithValue("@FunctionName", name);
                        cmd.Parameters.AddWithValue("@FunctionSchema", schema == null ? DBNull.Value : schema);
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
                                nullable = (bool)reader["nullable"]
                            });
                        }
                        result["table_columns"] = columns;
                    }
                }

                // Definition
                using (var cmd = new SqlCommand(DefinitionQuery, conn))
                {
                    cmd.Parameters.AddWithValue("@FunctionName", name);
                    cmd.Parameters.AddWithValue("@FunctionSchema", schema == null ? DBNull.Value : schema);
                    using var reader = await cmd.ExecuteReaderAsync();
                    if (await reader.ReadAsync())
                    {
                        result["definition"] = reader["definition"] is DBNull ? null : reader["definition"];
                    }
                }

                // Dependencies
                using (var cmd = new SqlCommand(DependenciesQuery, conn))
                {
                    cmd.Parameters.AddWithValue("@FunctionName", name);
                    cmd.Parameters.AddWithValue("@FunctionSchema", schema == null ? DBNull.Value : schema);
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
            _logger.LogError(ex, "DescribeFunction failed: {Message}", ex.Message);
            return new DbOperationResult(success: false, error: ex.Message);
        }
    }
}