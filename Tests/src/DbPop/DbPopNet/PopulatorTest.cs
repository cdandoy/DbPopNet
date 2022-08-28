using DbPop.DbPopNet.Fs;
using static System.Diagnostics.Debug;

namespace Tests.DbPop.DbPopNet;

[TestFixture]
public class PopulatorTest
{
    [Test]
    public void TestPopulator()
    {
        var sfs = LocalFileSystem.FindFromCurrentDirectory("Tests/resources/testdata");
        var datasets = sfs.List();
        Assert(datasets.Count == 1);
        var baseDataset = datasets[0];
        Assert(baseDataset.Name() == "base");
        
        var catalogs = baseDataset.List();
        Assert(catalogs.Count==1);
        var catalog = catalogs[0];
        Assert(catalog.Name()=="master");
        
        var schemas = catalog.List();
        Assert(schemas.Count==1);
        var schema = schemas[0];
        Assert(schema.Name()=="dbo");
        
        var tables = schema.List();
        Assert(tables.Count==1);
        var table = tables[0];
        Assert(table.Name()=="customer.csv");
    }
}