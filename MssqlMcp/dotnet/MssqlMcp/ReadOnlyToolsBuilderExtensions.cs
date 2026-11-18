// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;

namespace Mssql.McpServer;

public static class ReadOnlyToolsBuilderExtensions
{
    [RequiresUnreferencedCode("Tool discovery uses reflection and may not work in Native AOT.")]
    public static IMcpServerBuilder WithTools(
        this IMcpServerBuilder builder,
        IEnumerable<Type> toolTypes,
        bool readOnlyMode,
        JsonSerializerOptions? serializerOptions = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(toolTypes);

        foreach (var toolType in toolTypes)
        {
            if (toolType is null)
            {
                continue;
            }

            var methods = toolType.GetMethods(BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic);
            foreach (var toolMethod in methods)
            {
                var attribute = toolMethod.GetCustomAttribute<McpServerToolAttribute>();
                if (attribute is null || (readOnlyMode && !attribute.ReadOnly))
                {
                    continue;
                }

                _ = builder.Services.AddSingleton((Func<IServiceProvider, McpServerTool>)(services =>
                {
                    var options = new McpServerToolCreateOptions
                    {
                        Services = services,
                        SerializerOptions = serializerOptions
                    };

                    return toolMethod.IsStatic
                        ? McpServerTool.Create(toolMethod, (object?)null, options)
                        : McpServerTool.Create(toolMethod, toolType, options);
                }));
            }
        }

        return builder;
    }
}
