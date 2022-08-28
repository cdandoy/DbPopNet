namespace DbPop.DbPopNet.Db;

public class Column
{
    private readonly string _name;
    private readonly ColumnType _columnType;
    private readonly bool _nullable;
    private readonly bool _autoIncrement;

    public Column(string name, ColumnType columnType, bool nullable, bool autoIncrement)
    {
        _name = name;
        _columnType = columnType;
        _nullable = nullable;
        _autoIncrement = autoIncrement;
    }

    public string Name => _name;

    public ColumnType ColumnType => _columnType;

    public bool Nullable => _nullable;

    public bool AutoIncrement => _autoIncrement;
}