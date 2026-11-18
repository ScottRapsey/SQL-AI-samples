// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Mssql.McpServer;

internal class Program
{
    /// <summary>
    /// Entry point for the MCP server application.
    /// Sets up logging, configures the MCP server with standard I/O transport and tool discovery,
    /// builds the host, and runs the server asynchronously.
    /// </summary>
    /// <param name="args">Command-line arguments.</param>
    private static async Task Main(string[] args)
    {
        var readOnlyMode = GetReadOnlyMode();

        // Create the application host builder
        var builder = Host.CreateApplicationBuilder(args);

        // Configure console logging with Trace level
        _ = builder.Logging.AddConsole(consoleLogOptions =>
        {
            consoleLogOptions.LogToStandardErrorThreshold = LogLevel.Trace;
        });

        // Register ISqlConnectionFactory and Tools for DI
        _ = builder.Services.AddSingleton<ISqlConnectionFactory, SqlConnectionFactory>();
        _ = builder.Services.AddSingleton<Tools>();

        // Register MCP server and tools (instance-based)
        _ = builder.Services
            .AddMcpServer()
            .WithStdioServerTransport()
            .WithTools(new[] { typeof(Tools) }, readOnlyMode);

        // Build the host
        var host = builder.Build();
        host.Services.GetRequiredService<ILogger<Program>>()
            .LogInformation("MCP server read-only mode: {ReadOnlyMode}", readOnlyMode);

        // Setup cancellation token for graceful shutdown (Ctrl+C or SIGTERM)
        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (sender, eventArgs) =>
        {
            eventArgs.Cancel = true; // Prevent the process from terminating immediately
            cts.Cancel();
        };

        try
        {
            // Run the host with cancellation support
            await host.RunAsync(cts.Token);
        }
        catch (Exception ex)
        {
            // Attempt to log the exception using the host's logger
            if (host.Services.GetService(typeof(ILogger<Program>)) is ILogger<Program> logger)
            {
                logger.LogCritical(ex, "Unhandled exception occurred during host execution.");
            }
            else
            {
                Console.Error.WriteLine($"Unhandled exception: {ex}");
            }

            // Set a non-zero exit code
            Environment.ExitCode = 1;
        }
    }

    private static bool GetReadOnlyMode()
    {
        var value = Environment.GetEnvironmentVariable("READ_ONLY");
        if (value is null)
        {
            return false;
        }

        if (bool.TryParse(value, out var readOnlyMode))
        {
            return readOnlyMode;
        }

        throw new InvalidOperationException("Environment variable 'READ_ONLY' must be 'true' or 'false'.");
    }
}
