namespace DbPop.DbPopNet.Db;

public interface IDatabasePreparationStrategy
{
    void BeforeInserts();
    void AfterInserts();
}