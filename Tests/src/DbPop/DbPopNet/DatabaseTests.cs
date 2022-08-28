using System.Data.SqlClient;
using DbPop.DbPopNet;
using DbPop.DbPopNet.Db;
using static System.Diagnostics.Debug;

namespace Tests.DbPop.DbPopNet;

public class DatabaseTests
{
    [Test]
    public void TestDatabase()
    {
        var env = Env.CreateEnv().GetEnvironment("mssql");
        var connectionString = env.GetString("connectionString");
        var username = env.GetString("username");
        var password = env.GetString("password");

        var builder = new SqlConnectionStringBuilder(connectionString)
        {
            UserID = username,
            Password = password
        };
        using var sqlConnection = new SqlConnection(builder.ConnectionString);
        sqlConnection.Open();
        try
        {
            using var database = Database.CreateDatabase(sqlConnection);
        }
        finally
        {
            sqlConnection.Close();
        }

        WriteLine("HELO");
    }
}