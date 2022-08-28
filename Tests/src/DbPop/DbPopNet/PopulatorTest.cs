using DbPop.DbPopNet.Upload;
using NUnit.Framework;

namespace Tests.DbPop.DbPopNet;

[TestFixture]
public class PopulatorTest
{
    [Test]
    public void TestPopulator()
    {
        Populator.Instance.Load("base");
    }
}