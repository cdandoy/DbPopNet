using System.Data;
using System.Data.SqlClient;

namespace DbPop.DbPopNet.Database.MsSql;

public class SqlServerDatabase : Database
{
    protected internal SqlServerDatabase(SqlConnection connection, DatabaseMetaData metaData) : base(connection, metaData)
    {
    }

    public override List<TableName> GetTableNames(string catalog)
    {
        // TABLE_CATALOG String Catalog of the table.
        // TABLE_SCHEMA  String Schema that contains the table.
        // TABLE_NAME    String Table name.
        // TABLE_TYPE    String Type of table. Can be VIEW or BASE TABLE.
        var tableNames = new List<TableName>();
        Use(catalog);
        var dataTable = Connection.GetSchema("Tables", new[] { catalog });
        foreach (DataRow row in dataTable.Rows)
        {
            var tableSchema = (string)row[1];
            var tableName = (string)row[2];
            var tableType = (string)row[3];
            if ("BASE TABLE" == tableType)
                tableNames.Add(new TableName(catalog, tableSchema, tableName));
        }

        return tableNames;
    }

    public override List<TableName> GetTableNames(string catalog, string schema)
    {
        // TABLE_CATALOG String Catalog of the table.
        // TABLE_SCHEMA  String Schema that contains the table.
        // TABLE_NAME    String Table name.
        // TABLE_TYPE    String Type of table. Can be VIEW or BASE TABLE.
        var tableNames = new List<TableName>();
        Use(catalog);
        
        var dataTable = Connection.GetSchema("Tables", new[] { catalog });
        foreach (DataRow row in dataTable.Rows)
        {
            var tableSchema = (string)row[1];
            var tableName = (string)row[2];
            var tableType = (string)row[3];
            if ("BASE TABLE" == tableType && schema == tableSchema)
                tableNames.Add(new TableName(catalog, schema, tableName));
        }

        return tableNames;
    }

    private void Use(string catalog)
    {
        new SqlCommand("USE " + catalog, Connection).ExecuteNonQuery();
    }
}