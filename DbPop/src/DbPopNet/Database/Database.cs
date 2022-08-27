using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Text.RegularExpressions;
using DbPop.DbPopNet.Database.MsSql;

namespace DbPop.DbPopNet.Database;

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