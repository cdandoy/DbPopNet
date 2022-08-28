using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Text.RegularExpressions;
using CsvHelper;
using DbPop.DbPopNet.Db.MsSql;
using DbPop.DbPopNet.Upload;

namespace DbPop.DbPopNet.Db;

public abstract class Database : IDisposable
{
    protected readonly SqlConnection Connection;
    private readonly string _identifierQuoteString;

    protected Database(SqlConnection connection, DatabaseMetaData metaData)
    {
        Connection = connection;
        _identifierQuoteString = GetIdentifierQuoteString(metaData.QuotedIdentifierPattern);
    }

    public void Dispose()
    {
        Connection.Dispose();
    }

    private static string GetIdentifierQuoteString(string? identifierPattern)
    {
        if (identifierPattern == null) return "\"";
        var regex = new Regex(identifierPattern, RegexOptions.Compiled);
        return regex.Match("\"xxxx\"").Success ? "\"" :
            regex.Match("`xxx`").Success ? "`" :
            regex.Match("'xxx'").Success ? "'" : "\"";
    }

    public static Database CreateDatabase(SqlConnection sqlConnection)
    {
        var metaData = new DatabaseMetaData(sqlConnection);
        var productName = metaData.DataSourceProductName;
        if ("Microsoft SQL Server".Equals(productName))
            return new SqlServerDatabase(sqlConnection, metaData);

        throw new Exception("Unsupported database " + productName);
    }

    public SqlConnection GetConnection()
    {
        return Connection;
    }

    public string Quote(TableName tableName)
    {
        var sb = new StringBuilder();
        if (tableName.Catalog != null)
            sb.Append(Quote(tableName.Catalog)).Append('.');
        if (tableName.Schema != null)
            sb.Append(Quote(tableName.Schema)).Append('.');
        sb.Append(Quote(tableName.Table));
        return sb.ToString();
    }

    private string Quote(string? s)
    {
        if (s == null)
            return "";
        s = s.Replace(_identifierQuoteString, "\\" + _identifierQuoteString);
        return _identifierQuoteString + s + _identifierQuoteString;
    }

    public bool IsBinary(Type? dataType)
    {
        return dataType == typeof(byte[]);
    }

    public List<string> GetCatalogs()
    {
        var dataTable = Connection.GetSchema("Databases");
        return (from DataRow row in dataTable.Rows select (string)row[0]).ToList();
    }

    public abstract List<TableName> GetTableNames(string catalog);
    public abstract List<TableName> GetTableNames(string catalog, string schema);

    public List<Table> Tables(ISet<TableName> tableNames)
    {
        throw new NotImplementedException();
    }

    public void DeleteTable(Table table)
    {
        DeleteTable(table.TableName);
    }

    private void DeleteTable(TableName tableName)
    {
        new SqlCommand($"DELETE FROM ${Quote(tableName)}")
            .ExecuteNonQuery();
    }

    public virtual IDatabasePreparationStrategy CreateDatabasePreparationStrategy(ICollection<Table> allTables) => new DefaultDatabasePreparationStrategy(this, allTables);

    private class DefaultDatabasePreparationStrategy : IDatabasePreparationStrategy
    {
        private readonly Database _database;
        private readonly ICollection<Table> _allTables;

        public DefaultDatabasePreparationStrategy(Database database, ICollection<Table> allTables)
        {
            _allTables = allTables;
            _database = database;
        }

        public void BeforeInserts()
        {
            foreach (var table in _allTables)
                _database.DeleteTable(table);
        }

        public void AfterInserts()
        {
        }
    }

    public DatabaseInserter CreateInserter(Table table, List<DataFileHeader> dataFileHeaders) => new(this, table, dataFileHeaders);
}

public class DatabaseInserter : IDisposable
{
    private const int BatchSize = 10000;
    private readonly IList<DataFileHeader> _dataFileHeaders;
    private readonly IList<int> _binaryColumns = new List<int>();
    private readonly IList<ColumnType> _columnTypes;
    private readonly DataTable _dataTable = new();
    private readonly SqlBulkCopy _sqlBulkCopy;

    public DatabaseInserter(Database database, Table table, List<DataFileHeader> dataFileHeaders)
    {
        _dataFileHeaders = dataFileHeaders;
        var sqlConnection = database.GetConnection();

        _sqlBulkCopy = new SqlBulkCopy(sqlConnection);
        _sqlBulkCopy.DestinationTableName = database.Quote(table.TableName);
        dataFileHeaders.ForEach(it => _sqlBulkCopy.ColumnMappings.Add(it.ColumnName, it.ColumnName));

        var columns = table.Columns;
        _columnTypes = dataFileHeaders
            .Select(dataFileHeader =>
            {
                var columnName = dataFileHeader.ColumnName;
                var column = columns.First(it => it.Name == columnName);
                if (column == null) throw new Exception($"Column doesn't exist: {columnName}");
                return column.ColumnType;
            })
            .ToList();
        for (var i = 0; i < dataFileHeaders.Count; i++)
        {
            if (dataFileHeaders[i].Binary)
                _binaryColumns.Add(i);
        }
    }

    public void Dispose()
    {
        Flush();
    }

    private void Flush()
    {
        _sqlBulkCopy.WriteToServer(_dataTable);
        _dataTable.Clear();
    }

    public void Insert(CsvReader csvReader)
    {
        var dataRow = _dataTable.NewRow();
        for (var i = 0; i < _dataFileHeaders.Count; i++)
        {
            var s = csvReader[i];
            if (s == null)
                dataRow[i] = DBNull.Value;
            else
            {
                if (_binaryColumns.Contains(i))
                {
                    var data = Convert.FromBase64String(s);
                    s = Encoding.UTF8.GetString(data);
                }

                var columnType = _columnTypes[i];
                dataRow[i] = columnType.Value(s);
            }
        }

        _dataTable.Rows.Add(dataRow);

        if (_dataTable.Rows.Count > BatchSize)
            Flush();
    }
}

public class DatabaseMetaData
{
    internal string? DataSourceProductName { get; }
    internal string? QuotedIdentifierPattern { get; }

    /*
    internal string? CompositeIdentifierSeparatorPattern { get; }
    internal string? DataSourceProductVersion { get; }
    internal string? DataSourceProductVersionNormalized { get; }
    internal string? GroupByBehavior { get; }
    internal string? IdentifierPattern { get; }
    internal string? IdentifierCase { get; }
    internal string? OrderByColumnsInSelect { get; }
    internal string? ParameterMarkerFormat { get; }
    internal string? ParameterMarkerPattern { get; }
    internal string? ParameterNameMaxLength { get; }
    internal string? ParameterNamePattern { get; }
    internal string? QuotedIdentifierCase { get; }
    internal string? StatementSeparatorPattern { get; }
    internal string? StringLiteralPattern { get; }
    internal string? SupportedJoinOperators { get; }
    */

    public DatabaseMetaData(SqlConnection connection)
    {
        var table = connection.GetSchema("DataSourceInformation");
        var dataRow = table.Rows[0];
        DataSourceProductName = dataRow["DataSourceProductName"].ToString();
        QuotedIdentifierPattern = dataRow["QuotedIdentifierPattern"].ToString();

        /*
        CompositeIdentifierSeparatorPattern = dataRow["CompositeIdentifierSeparatorPattern"].ToString();
        DataSourceProductVersion = dataRow["DataSourceProductVersion"].ToString();
        DataSourceProductVersionNormalized = dataRow["DataSourceProductVersionNormalized"].ToString();
        GroupByBehavior = dataRow["GroupByBehavior"].ToString();
        IdentifierPattern = dataRow["IdentifierPattern"].ToString();
        IdentifierCase = dataRow["IdentifierCase"].ToString();
        OrderByColumnsInSelect = dataRow["OrderByColumnsInSelect"].ToString();
        ParameterMarkerFormat = dataRow["ParameterMarkerFormat"].ToString();
        ParameterMarkerPattern = dataRow["ParameterMarkerPattern"].ToString();
        ParameterNameMaxLength = dataRow["ParameterNameMaxLength"].ToString();
        ParameterNamePattern = dataRow["ParameterNamePattern"].ToString();
        QuotedIdentifierCase = dataRow["QuotedIdentifierCase"].ToString();
        StatementSeparatorPattern = dataRow["StatementSeparatorPattern"].ToString();
        StringLiteralPattern = dataRow["StringLiteralPattern"].ToString();
        SupportedJoinOperators = dataRow["SupportedJoinOperators"].ToString();
        */
    }
}