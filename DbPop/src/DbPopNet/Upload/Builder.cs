using DbPop.DbPopNet.Fs;

namespace DbPop.DbPopNet.Upload;

public class Builder
{
    public string? Env { get; set; }
    public string ResourceDirectory { get; set; } = LocalFileSystem.DefaultDirectory;
    public string? ConnectionString { get; set; }
    public string? Username { get; set; }
    public string? Password { get; set; }

    public Populator Build()
    {
        return Populator.Build(this);
    }

    public SimpleFileSystem SimpleFileSystem()
    {
        return LocalFileSystem.FindFromCurrentDirectory(ResourceDirectory);
    }
}