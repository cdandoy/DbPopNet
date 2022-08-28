using System.Data;
using System.Data.SqlClient;

namespace DbPop.DbPopNet.Db.MsSql;

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

    public override IDatabasePreparationStrategy CreateDatabasePreparationStrategy(ICollection<Table> allTables) => new SqlServerDisablePreparationStrategy(this, allTables);

    private void DisableForeignKey(ForeignKey foreignKey)
    {
        new SqlCommand(
                $"ALTER TABLE ${Quote(foreignKey.FkTableName)} NOCHECK CONSTRAINT ${foreignKey.Name}",
                Connection)
            .ExecuteNonQuery();
    }
    private void EnableForeignKey(ForeignKey foreignKey)
    {
        new SqlCommand(
                $"ALTER TABLE ${Quote(foreignKey.FkTableName)} WITH NOCHECK CHECK CONSTRAINT ${foreignKey.Name}",
                Connection)
            .ExecuteNonQuery();
    }

    private class SqlServerDisablePreparationStrategy : IDatabasePreparationStrategy
    {
        private readonly SqlServerDatabase _database;
        private readonly ICollection<Table> _allTables;
        private readonly ISet<ForeignKey> _foreignKeys;

        public SqlServerDisablePreparationStrategy(SqlServerDatabase database, ICollection<Table> allTables)
        {
            _database = database;
            _allTables = allTables;
            _foreignKeys = allTables
                .SelectMany(table => table.ForeignKeys)
                .ToHashSet();
        }

        public void BeforeInserts()
        {
            foreach (var table in _allTables)
                _database.DeleteTable(table);

            foreach (var foreignKey in _foreignKeys)
                _database.DisableForeignKey(foreignKey);
        }

        public void AfterInserts()
        {
            foreach (var foreignKey in _foreignKeys)
                _database.EnableForeignKey(foreignKey);
        }
    }
}