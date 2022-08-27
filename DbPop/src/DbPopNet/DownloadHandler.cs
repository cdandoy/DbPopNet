using System.Data.SqlClient;
using DbPop.DbPopNet.Database;
using DbPop.DbPopNet.download;

namespace DbPop.DbPopNet;

internal static class DownloadHandler
{
    public static void Handle(
        SqlConnection sqlConnection,
        DirectoryInfo directory,
        string dataset,
        List<string> tables
    )
    {
        using var downloader = new Downloader(Database.Database.CreateDatabase(sqlConnection), directory, dataset);

        foreach (var table in tables)
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
    }
}