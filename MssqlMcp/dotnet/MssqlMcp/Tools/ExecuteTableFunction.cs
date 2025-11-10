// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.ComponentModel;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;

namespace Mssql.McpServer;

public partial class Tools
{
    [McpServerTool(
        Title = "Execute Table Function",
        ReadOnly = true,
        Idempotent = true,
        Destructive = false),
        Description("Executes a table-valued function and returns the result set")]
    public async Task<DbOperationResult> ExecuteTableFunction(
        [Description("Name of table-valued function (can include schema, e.g., 'dbo.MyTableFunction')")] string name,
        [Description("Optional JSON object containing parameter names and values, e.g., {\"@StartDate\": \"2024-01-01\", \"@EndDate\": \"2024-12-31\"}")] string? parameters = null,
        [Description("Optional database name. If not specified, uses the default database from connection string.")] string? database = null)
    {
        var schema = "dbo";
        if (name.Contains('.'))
        {
            var parts = name.Split('.');
            if (parts.Length > 1)
            {
                schema = parts[0];
                name = parts[1];
            }
        }

        var funcName = schema != null ? $"[{schema}].[{name}]" : $"[{name}]";

        var conn = database == null
            ? await _connectionFactory.GetOpenConnectionAsync()
            : await _connectionFactory.GetOpenConnectionAsync(database);

        try
        {
            using (conn)
            {
                using var cmd = new SqlCommand();
                cmd.Connection = conn;

                // Parse parameters
                if (string.IsNullOrEmpty(parameters))
                {
                    // No parameters - simple case
                    cmd.CommandText = $"SELECT * FROM {funcName}()";
                }
                else
                {
                    try
                    {
                        var paramDict = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(parameters);
                        if (paramDict == null || paramDict.Count == 0)
                        {
                            cmd.CommandText = $"SELECT * FROM {funcName}()";
                        }
                        else
                        {
                            // Build SQL with DECLARE statements and variable assignments
                            var declares = new List<string>();
                            var sets = new List<string>();
                            var functionParams = new List<string>();
                            var paramIndex = 0;

                            foreach (var param in paramDict)
                            {
                                var varName = $"@v{paramIndex}";      // SQL variable: @v0, @v1
                                var paramName = $"@p{paramIndex}";    // ADO.NET parameter: @p0, @p1
                                var value = ConvertJsonElementToObject(param.Value);

                                // Add ADO.NET parameter
                                _ = cmd.Parameters.AddWithValue(paramName, value ?? DBNull.Value);

                                // Build DECLARE and SET statements
                                var sqlType = GetSqlTypeForValue(value);
                                declares.Add($"DECLARE {varName} {sqlType}");
                                sets.Add($"SET {varName} = {paramName}");  // SET @v0 = @p0
                                functionParams.Add(varName);

                                paramIndex++;
                            }

                            // Build the complete SQL batch
                            var sql = string.Join("; ", declares) + "; " +
                                     string.Join("; ", sets) + "; " +
                                     $"SELECT * FROM {funcName}({string.Join(", ", functionParams)})";

                            cmd.CommandText = sql;
                        }
                    }
                    catch (Exception ex)
                    {
                        return new DbOperationResult(success: false, error: $"Invalid parameter JSON: {ex.Message}");
                    }
                }

                using var reader = await cmd.ExecuteReaderAsync();
                var results = new List<Dictionary<string, object?>>();

                while (await reader.ReadAsync())
                {
                    var row = new Dictionary<string, object?>();
                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        var value = reader.GetValue(i);
                        row[reader.GetName(i)] = value is DBNull ? null : value;
                    }
                    results.Add(row);
                }

                return new DbOperationResult(success: true, data: results);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ExecuteTableFunction failed: {Message}", ex.Message);
            return new DbOperationResult(success: false, error: ex.Message);
        }
    }
}