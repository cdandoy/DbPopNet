using System.Collections.ObjectModel;

namespace DbPop.DbPopNet.Db;

public class ForeignKey
{
    private readonly string _name;
    private readonly string _constraintDef;
    private readonly TableName _pkTableName;
    private readonly List<string> _pkColumns;
    private readonly TableName _fkTableName;
    private readonly List<string> _fkColumns;

    public ForeignKey(string name, string constraintDef, TableName pkTableName, List<string> pkColumns, TableName fkTableName, List<string> fkColumns)
    {
        _name = name;
        _constraintDef = constraintDef;
        _pkTableName = pkTableName;
        _pkColumns = pkColumns;
        _fkTableName = fkTableName;
        _fkColumns = fkColumns;
    }

    public string Name => _name;

    public string ConstraintDef => _constraintDef;

    public TableName PkTableName => _pkTableName;

    public ReadOnlyCollection<string> PkColumns => _pkColumns.AsReadOnly();

    public TableName FkTableName => _fkTableName;

    public ReadOnlyCollection<string> FkColumns => _fkColumns.AsReadOnly();
}