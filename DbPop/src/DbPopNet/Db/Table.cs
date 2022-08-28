namespace DbPop.DbPopNet.Db;

public class Table
{
    public TableName TableName { get; }
    public List<Column> Columns { get; }
    public List<Index> Indexes { get; }
    public List<ForeignKey> ForeignKeys { get; }

    public Table(TableName tableName, List<Column> columns, List<Index> indexes, List<ForeignKey> foreignKeys)
    {
        TableName = tableName;
        Columns = columns;
        Indexes = indexes;
        ForeignKeys = foreignKeys;
    }
}