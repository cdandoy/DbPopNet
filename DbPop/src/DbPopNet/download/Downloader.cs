using System.Collections.ObjectModel;
using System.Data.Common;
using System.Data.SqlClient;
using System.Globalization;
using CsvHelper;
using DbPop.DbPopNet.Database;

namespace DbPop.DbPopNet.download;

public class Downloader : IDisposable
{
    private const int MaxLength = 1024 * 32;
    private readonly Database.Database _database;
    private readonly DirectoryInfo _directory;

    public Downloader(Database.Database database, DirectoryInfo directory, string dataset)
    {
        _database = database;
        _directory = directory.CreateSubdirectory(dataset);
    }

    public void Dispose()
    {
        _database.Dispose();
    }

    public Database.Database GetDatabase()
    {
        return _database;
    }

    public void Download(TableName tableName)
    {
        var quotedTableName = _database.Quote(tableName);
        var connection = _database.GetConnection();
        using var sqlCommand = new SqlCommand($"select * from {quotedTableName}", connection);
        using var writer = GetFileWriter(tableName);
        using var csvWriter = new CsvWriter(writer, CultureInfo.InvariantCulture);
        using var sqlDataReader = sqlCommand.ExecuteReader();
        var fieldCount = sqlDataReader.FieldCount;
        var columnSchema = sqlDataReader.GetColumnSchema();
        for (var i = 0; i < fieldCount; i++)
        {
            if (columnSchema[i].DataType == null) continue; // We don't know (yet) how to download this

            var columnName = columnSchema[i].ColumnName;
            if (_database.IsBinary(columnSchema[i].DataType))
                columnName += "*b64";
            csvWriter.WriteField(columnName);
        }

        csvWriter.NextRecord();

        while (sqlDataReader.Read())
        {
            for (var i = 0; i < fieldCount; i++)
            {
                if (sqlDataReader.IsDBNull(i))
                {
                    csvWriter.WriteField(null);
                    continue;
                }

                var dataType = columnSchema[i].DataType;
                if (dataType == null) continue;
                if (dataType == typeof(string))
                {
                    if (columnSchema[i].ColumnSize > MaxLength)
                        DownloadLargeString(tableName, sqlDataReader, csvWriter, columnSchema, i);
                    else
                        DownloadString(sqlDataReader, csvWriter, i);
                }
                else if (dataType == typeof(int))
                    DownloadInt32(sqlDataReader, csvWriter, i);
                else if (dataType == typeof(bool))
                    DownloadBoolean(sqlDataReader, csvWriter, i);
                else if (dataType == typeof(short))
                    DownloadInt16(sqlDataReader, csvWriter, i);
                else if (dataType == typeof(long))
                    DownloadInt64(sqlDataReader, csvWriter, i);
                else if (dataType == typeof(DateTime))
                    DownloadDateTime(sqlDataReader, csvWriter, i);
                else if (dataType == typeof(TimeSpan))
                    DownloadTimeSpan(sqlDataReader, csvWriter, i);
                else if (dataType == typeof(byte[]))
                    DownloadBinary(tableName, sqlDataReader, csvWriter, columnSchema, i);
                else if (dataType == typeof(decimal))
                    DownloadDecimal(sqlDataReader, csvWriter, i);
                else if (dataType == typeof(byte))
                    DownloadByte(sqlDataReader, csvWriter, i);
                else if (dataType == typeof(double))
                    DownloadDouble(sqlDataReader, csvWriter, i);
                else if (dataType == typeof(Guid))
                    DownloadGuid(sqlDataReader, csvWriter, i);
                else if (dataType == typeof(char))
                    DownloadChar(sqlDataReader, csvWriter, i);
                else if (dataType == typeof(char[]))
                    DownloadChars(sqlDataReader, csvWriter, i);
                else
                    DownloadString(sqlDataReader, csvWriter, i);
            }

            csvWriter.NextRecord();
        }
    }

    private static void DownloadInt16(SqlDataReader sqlDataReader, CsvWriter csvWriter, int i)
    {
        var v = sqlDataReader.GetInt16(i);
        csvWriter.WriteField(v);
    }

    private static void DownloadInt32(SqlDataReader sqlDataReader, CsvWriter csvWriter, int i)
    {
        var v = sqlDataReader.GetInt32(i);
        csvWriter.WriteField(v);
    }

    private static void DownloadInt64(SqlDataReader sqlDataReader, CsvWriter csvWriter, int i)
    {
        var v = sqlDataReader.GetInt64(i);
        csvWriter.WriteField(v);
    }

    private static void DownloadBoolean(SqlDataReader sqlDataReader, CsvWriter csvWriter, int i)
    {
        var v = sqlDataReader.GetBoolean(i);
        csvWriter.WriteField(v);
    }

    private static void DownloadByte(SqlDataReader sqlDataReader, CsvWriter csvWriter, int i)
    {
        var v = sqlDataReader.GetByte(i);
        csvWriter.WriteField(v);
    }

    private static void DownloadDecimal(SqlDataReader sqlDataReader, CsvWriter csvWriter, int i)
    {
        var s = sqlDataReader.GetDecimal(i).ToString(CultureInfo.InvariantCulture);
        csvWriter.WriteField(s);
    }

    private static void DownloadDouble(SqlDataReader sqlDataReader, CsvWriter csvWriter, int i)
    {
        var s = sqlDataReader.GetDouble(i).ToString(CultureInfo.InvariantCulture);
        csvWriter.WriteField(s);
    }

    private static void DownloadGuid(SqlDataReader sqlDataReader, CsvWriter csvWriter, int i)
    {
        var s = sqlDataReader.GetGuid(i).ToString();
        csvWriter.WriteField(s);
    }

    private static void DownloadChar(SqlDataReader sqlDataReader, CsvWriter csvWriter, int i)
    {
        var s = sqlDataReader.GetChar(i).ToString();
        csvWriter.WriteField(s);
    }

    private static void DownloadChars(SqlDataReader sqlDataReader, CsvWriter csvWriter, int i)
    {
        var sqlChars = sqlDataReader.GetSqlChars(i);
        var chars = sqlChars.Value;
        csvWriter.WriteField(chars);
    }

    private static void DownloadDateTime(SqlDataReader sqlDataReader, CsvWriter csvWriter, int i)
    {
        var s = sqlDataReader.GetDateTime(i);
        csvWriter.WriteField(s);
    }

    private static void DownloadTimeSpan(SqlDataReader sqlDataReader, CsvWriter csvWriter, int i)
    {
        var s = sqlDataReader.GetTimeSpan(i);
        csvWriter.WriteField(s);
    }

    private static void DownloadBinary(TableName tableName, SqlDataReader sqlDataReader, CsvWriter csvWriter, ReadOnlyCollection<DbColumn> columnSchema, int i)
    {
        var sqlBinary = sqlDataReader.GetSqlBytes(i);
        if (sqlBinary.Length > MaxLength)
        {
            DownloadTooLarge(tableName, columnSchema[i].ColumnName, sqlBinary.Length);
        }
        else
        {
            var bytes = sqlBinary.Value;
            var base64String = Convert.ToBase64String(bytes);
            csvWriter.WriteField(base64String);
        }
    }

    private static void DownloadTooLarge(TableName tableName, string columnName, long length)
    {
        Console.Out.WriteLine($"Data too large : {tableName.ToQualifiedName()}.{columnName} - {length / 1024}Kb");
    }

    private static void DownloadLargeString(TableName tableName, SqlDataReader sqlDataReader, CsvWriter csvWriter, ReadOnlyCollection<DbColumn> columnSchema, int i)
    {
        var sqlChars = sqlDataReader.GetSqlChars(i);
        if (sqlChars.Length > MaxLength)
        {
            DownloadTooLarge(tableName, columnSchema[i].ColumnName, sqlChars.Length);
        }
        else
        {
            DownloadString(sqlDataReader, csvWriter, i);
        }
    }

    private static void DownloadString(SqlDataReader sqlDataReader, CsvWriter csvWriter, int i)
    {
        var s = sqlDataReader.GetString(i);
        csvWriter.WriteField(s);
    }

    private StreamWriter GetFileWriter(TableName tableName)
    {
        var directory = _directory;
        if (tableName.Catalog != null)
            directory = directory.CreateSubdirectory(tableName.Catalog);
        if (tableName.Schema != null)
            directory = directory.CreateSubdirectory(tableName.Schema);
        var fileInfo = new FileInfo(directory.FullName + "\\" + tableName.Table + ".csv");
        return fileInfo.CreateText();
    }
}