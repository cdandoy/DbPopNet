using DbPop.DbPopNet;

namespace Tests.DbPop.DbPopNet;

public class EnvTests
{
    [Test]
    public void Test1()
    {
        var tempFileName = Path.GetTempFileName();
        try
        {
            var fileInfo = new FileInfo(tempFileName);
            using (var streamWriter = fileInfo.CreateText())
            {
                streamWriter.Write(@"one=un
two=deux
spanish.one=uno
spanish.two=dos
deutch.one=een
deutch.two=twee
");
            }

            var env = Env.CreateEnv(new FileInfo(tempFileName));
            Assert.Multiple(() =>
            {
                Assert.That(env, Is.Not.Null);
                Assert.That(env.GetString("one"), Is.EqualTo("un"));
                Assert.That(env.GetString("two"), Is.EqualTo("deux"));
                Assert.That(env.GetEnvironment("spanish").GetString("one"), Is.EqualTo("uno"));
                Assert.That(env.GetEnvironment("spanish").GetString("two"), Is.EqualTo("dos"));
                Assert.That(env.GetEnvironment("deutch").GetString("one"), Is.EqualTo("een"));
                Assert.That(env.GetEnvironment("deutch").GetString("two"), Is.EqualTo("twee"));
            });
        }
        finally
        {
            File.Delete(tempFileName);
        }
    }
}