using System.Text;

namespace DbPop.DbPopNet.Db;

public class TableName
{
    public readonly string? Catalog;
    public readonly string? Schema;
    public readonly string Table;

    public TableName(string? catalog, string? schema, string table)
    {
        Catalog = catalog;
        Schema = schema;
        Table = table;
    }

    public string ToQualifiedName()
    {
        var sb = new StringBuilder();
        if (Catalog != null)
            sb.Append(Catalog).Append('.');
        if (Schema != null)
            sb.Append(Schema).Append('.');
        sb.Append(Table);
        return sb.ToString();
    }
}