using System.Collections.ObjectModel;

namespace DbPop.DbPopNet.Db;

public class Index
{
    private readonly string _name;
    private readonly TableName _tableName;
    private readonly bool _unique;
    private readonly bool _primaryKey;
    private readonly List<string> _columns;

    public Index(string name, TableName tableName, bool unique, bool primaryKey, List<string> columns)
    {
        _name = name;
        _tableName = tableName;
        _unique = unique;
        _primaryKey = primaryKey;
        _columns = columns;
    }

    public string Name => _name;

    public TableName TableName => _tableName;

    public bool Unique => _unique;

    public bool PrimaryKey => _primaryKey;

    public ReadOnlyCollection<string> Columns => _columns.AsReadOnly();
}