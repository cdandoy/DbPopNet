using System.CommandLine;
using System.Data.SqlClient;
using DbPop.DbPopNet.Db;
using DbPop.DbPopNet.download;

namespace DbPop.DbPopNet;

internal static class DbPop
{
    private static readonly Option<string> ConnectionString = new("--connection", "The connection string to the database.");
    private static readonly Option<string> Username = new("--username", "The database username");
    private static readonly Option<string> Password = new("--password", "The database password");
    private static readonly Option<string> Environment = new("--environment", "The environment");
    private static readonly Env Env = Env.CreateEnv();

    private static async Task Main(string[] args)
    {
        var rootCommand = new RootCommand();

        rootCommand.AddGlobalOption(ConnectionString);
        rootCommand.AddGlobalOption(Username);
        rootCommand.AddGlobalOption(Password);

        var populateCommand = BuildPopulateCommand();
        rootCommand.Add(populateCommand);

        var downloadCommand = BuildDownloadCommand();
        rootCommand.Add(downloadCommand);

        await rootCommand.InvokeAsync(args);
    }

    private static Command BuildPopulateCommand()
    {
        var command = new Command("populate", "Populates the database with the content of the CSV files in the specified datasets");
        var path = new Option<string>("--path", "Dataset path");
        var datasets = new Argument<List<string>>(name: "datasets");
        command.SetHandler((connectionStringValue, usernameValue, passwordValue, environmentValue, pathValue, datasetsValue) =>
        {
            using var sqlConnection = CreateConnection(environmentValue, connectionStringValue, usernameValue, passwordValue);
            new PopulateHandler().Handle(pathValue, datasetsValue, environmentValue);
        }, ConnectionString, Username, Password, Environment, path, datasets);
        return command;
    }

    private static Command BuildDownloadCommand()
    {
        var command = new Command("download", "Download data to CSV files");
        var directory = new Option<DirectoryInfo?>("--directory", "Dataset Directory");
        var dataset = new Option<string>("--dataset", "Dataset") { IsRequired = true };
        var tables = new Argument<List<string>>(name: "tables");
        command.Add(directory);
        command.Add(dataset);
        command.Add(tables);

        command.SetHandler((connectionStringValue, usernameValue, passwordValue, environmentValue, directoryValue, datasetValue, tablesValue) =>
        {
            using var sqlConnection = CreateConnection(environmentValue, connectionStringValue, usernameValue, passwordValue);
            using var downloader = new Downloader(
                Database.CreateDatabase(sqlConnection),
                directoryValue ?? new DirectoryInfo("./output"),
                datasetValue
            );

            foreach (var table in tablesValue)
            {
                var split = table.Split('.');
                if (split.Length == 1)
                {
                    var catalog = split[0];
                    downloader.GetDatabase()
                        .GetTableNames(catalog)
                        .ForEach(downloader.Download);
                }
                else if (split.Length == 2)
                {
                    var catalog = split[0];
                    var schema = split[1];
                    downloader.GetDatabase()
                        .GetTableNames(catalog, schema)
                        .ForEach(downloader.Download);
                }
                else if (split.Length == 3)
                {
                    var schema = split[1];
                    var table1 = split[2];
                    downloader.Download(new TableName(split[0], schema, table1));
                }
                else
                    throw new Exception("Invalid database/schema/table: " + table);
            }
        }, ConnectionString, Username, Password, Environment, directory, dataset, tables);

        return command;
    }

    public static SqlConnection CreateConnection(string? environment, string? connectionString, string? username, string? password)
    {
        var env = Env.GetEnvironment(environment);
        var builder = new SqlConnectionStringBuilder(connectionString ?? env.GetString("connectionString"))
        {
            UserID = username ?? env.GetString("username"),
            Password = password ?? env.GetString("password")
        };
        var sqlConnection = new SqlConnection(builder.ConnectionString);
        sqlConnection.Open();
        return sqlConnection;
    }
}