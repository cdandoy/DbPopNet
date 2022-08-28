namespace DbPop.DbPopNet.Fs;

public abstract class SimpleFileSystem
{
    protected readonly string Path;

    protected SimpleFileSystem(string path)
    {
        Path = path;
    }

    protected static string TrimSlashes(string path)
    {
        if (path.StartsWith("/")) path = path[1..];
        if (path.EndsWith("/")) path = path[..^1];
        return path;
    }

    public string Name()
    {
        var i = Path.LastIndexOf('/');
        return Path[(i + 1)..];
    }

    public abstract SimpleFileSystem Cd(string subPath);
    public abstract List<SimpleFileSystem> List();
    public abstract TextReader TextReader();
}