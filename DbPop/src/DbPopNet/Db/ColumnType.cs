namespace DbPop.DbPopNet.Db;

public abstract class ColumnType
{
    public static readonly ColumnType Varchar = new VarcharColumnType();
    
/*
    public void bind(PreparedStatement preparedStatement, int jdbcPos, String input) throws SQLException {
        preparedStatement.setString(jdbcPos, input);
    }

    public void bind(PreparedStatement preparedStatement, int jdbcPos, byte[] input) throws SQLException {
        preparedStatement.setBytes(jdbcPos, input);
    }

    public void bind(PreparedStatement preparedStatement, int jdbcPos, Object input) throws SQLException {
        preparedStatement.setObject(jdbcPos, input);
    }
 */
}

internal class VarcharColumnType : ColumnType
{
}