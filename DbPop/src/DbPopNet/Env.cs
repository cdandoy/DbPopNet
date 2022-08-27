namespace DbPop.DbPopNet;

public class Env
{
    private readonly string _currentEnvironmentName;
    private readonly Dictionary<string, Dictionary<string, string>> _allEnvironments;

    private Env(string currentEnvironmentName, Dictionary<string, Dictionary<string, string>> allEnvironments)
    {
        _currentEnvironmentName = currentEnvironmentName;
        _allEnvironments = allEnvironments;
    }

    public Env GetEnvironment(string? environment)
    {
        environment ??= "default";
        return string.Equals(environment, _currentEnvironmentName)
            ? this
            : new Env(environment, _allEnvironments);
    }

    public string? GetString(string key, string? defaultValue = null)
    {
        if (!_allEnvironments.TryGetValue(_currentEnvironmentName, out var env))
            throw new Exception("Invalid environment: " + _currentEnvironmentName);

        return env.TryGetValue(key, out var s) ? s : defaultValue;
    }

    public static Env CreateEnv()
    {
        var homeFolderPath = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        if ("".Equals(homeFolderPath)) throw new Exception("Home directory not found");

        var fileInfo = new FileInfo(homeFolderPath + "/.dbpop/dbpopnet.properties");
        if (!fileInfo.Exists) throw new Exception("Default property file not found: " + fileInfo);

        return CreateEnv(fileInfo);
    }

    public static Env CreateEnv(FileInfo fileInfo)
    {
        var allEnvironments = new Dictionary<string, Dictionary<string, string>>();

        using var streamReader = fileInfo.OpenText();
        while (true)
        {
            var line = streamReader.ReadLine();
            if (line == null) break;
            if (line.Length == 0 ||
                line.StartsWith(";") ||
                line.StartsWith("#") ||
                line.StartsWith("'") ||
                !line.Contains('=')) continue;

            var index = line.IndexOf('=');
            var key = line[..index].Trim();
            var value = line[(index + 1)..].Trim();

            if ((value.StartsWith("\"") && value.EndsWith("\"")) ||
                (value.StartsWith("'") && value.EndsWith("'")))
            {
                value = value.Substring(1, value.Length - 2);
            }

            var env = "default";
            var dotPos = key.IndexOf('.');
            if (dotPos >= 0)
            {
                env = key[..dotPos];
                key = key[(dotPos + 1)..];
            }

            if (!allEnvironments.TryGetValue(env, out var dictionary))
            {
                dictionary = new Dictionary<string, string>();
                allEnvironments.Add(env, dictionary);
            }

            dictionary.TryAdd(key, value);
        }

        return new Env("default", allEnvironments);
    }
}