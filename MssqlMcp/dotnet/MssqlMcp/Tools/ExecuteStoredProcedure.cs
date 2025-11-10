// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.ComponentModel;
using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;

namespace Mssql.McpServer;

public partial class Tools
{
    [McpServerTool(
        Title = "Execute Stored Procedure",
        ReadOnly = false,
        Idempotent = false,
        Destructive = false),
        Description("Executes a stored procedure with optional parameters and returns results including output parameters and return value")]
    public async Task<DbOperationResult> ExecuteStoredProcedure(
        [Description("Name of stored procedure (can include schema, e.g., 'dbo.MyProc')")] string name,
        [Description("Optional JSON object containing parameter names and values, e.g., {\"@param1\": \"value1\", \"@param2\": 123}")] string? parameters = null,
        [Description("Optional database name. If not specified, uses the default database from connection string.")] string? database = null)
    {
        string? schema = null;
        if (name.Contains('.'))
        {
            var parts = name.Split('.');
            if (parts.Length > 1)
            {
                schema = parts[0];
                name = parts[1];
            }
        }

        var procName = schema != null ? $"[{schema}].[{name}]" : $"[{name}]";

        var conn = database == null
            ? await _connectionFactory.GetOpenConnectionAsync()
            : await _connectionFactory.GetOpenConnectionAsync(database);

        try
        {
            using (conn)
            {
                using var cmd = new SqlCommand(procName, conn)
                {
                    CommandType = CommandType.StoredProcedure
                };

                // Parse and add parameters if provided
                if (!string.IsNullOrEmpty(parameters))
                {
                    try
                    {
                        var paramDict = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(parameters);
                        if (paramDict != null)
                        {
                            foreach (var param in paramDict)
                            {
                                _ = cmd.Parameters.AddWithValue(param.Key, param.Value ?? DBNull.Value);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        return new DbOperationResult(success: false, error: $"Invalid parameter JSON: {ex.Message}");
                    }
                }

                // Add return value parameter
                var returnParam = cmd.Parameters.Add("@RETURN_VALUE", SqlDbType.Int);
                returnParam.Direction = ParameterDirection.ReturnValue;

                var result = new Dictionary<string, object>();
                var allResultSets = new List<List<Dictionary<string, object?>>>();

                // Execute and read all result sets
                using var reader = await cmd.ExecuteReaderAsync();

                do
                {
                    var resultSet = new List<Dictionary<string, object?>>();
                    while (await reader.ReadAsync())
                    {
                        var row = new Dictionary<string, object?>();
                        for (var i = 0; i < reader.FieldCount; i++)
                        {
                            var value = reader.GetValue(i);
                            row[reader.GetName(i)] = value is DBNull ? null : value;
                        }
                        resultSet.Add(row);
                    }
                    if (resultSet.Count > 0)
                    {
                        allResultSets.Add(resultSet);
                    }
                } while (await reader.NextResultAsync());

                // Get output parameters and return value
                var outputParams = new Dictionary<string, object?>();
                foreach (SqlParameter param in cmd.Parameters)
                {
                    if (param.Direction == ParameterDirection.Output ||
                        param.Direction == ParameterDirection.InputOutput ||
                        param.Direction == ParameterDirection.ReturnValue)
                    {
                        outputParams[param.ParameterName] = param.Value is DBNull ? null : param.Value;
                    }
                }

                result["return_value"] = returnParam.Value;

                if (allResultSets.Count == 1)
                {
                    result["result_set"] = allResultSets[0];
                }
                else if (allResultSets.Count > 1)
                {
                    result["result_sets"] = allResultSets;
                }

                if (outputParams.Count > 1) // More than just return value
                {
                    result["output_parameters"] = outputParams;
                }

                return new DbOperationResult(success: true, data: result);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ExecuteStoredProcedure failed: {Message}", ex.Message);
            return new DbOperationResult(success: false, error: ex.Message);
        }
    }
}