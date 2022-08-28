namespace DbPop.DbPopNet.Fs;

public class LocalFileSystem : SimpleFileSystem
{
    public const string DefaultDirectory = "Tests/resources/testdata";

    private readonly DirectoryInfo _directoryInfo;

    private LocalFileSystem(DirectoryInfo directoryInfo, string path) : base(path)
    {
        _directoryInfo = directoryInfo;
    }

    public static SimpleFileSystem FromPath(string path)
    {
        return new LocalFileSystem(new DirectoryInfo(path), "");
    }

    public static SimpleFileSystem FindFromCurrentDirectory(string path)
    {
        return new LocalFileSystem(FindRootDirectoryInfo(path), "");
    }

    public static DirectoryInfo FindRootDirectoryInfo()
    {
        return FindRootDirectoryInfo(DefaultDirectory);
    }

    private static DirectoryInfo FindRootDirectoryInfo(string path)
    {
        var currentDirectory = Directory.GetCurrentDirectory();
        var directoryInfo = new DirectoryInfo(currentDirectory);
        while (directoryInfo != null)
        {
            var sub = new DirectoryInfo(directoryInfo.FullName + "/" + TrimSlashes(path));
            if (sub.Exists)
                return sub;
            directoryInfo = directoryInfo.Parent;
        }

        throw new Exception($"Directory not found: .../{path}");
    }

    public override LocalFileSystem Cd(string subPath)
    {
        return new LocalFileSystem(_directoryInfo, Path + "/" + TrimSlashes(subPath));
    }

    public override List<SimpleFileSystem> List()
    {
        return new DirectoryInfo(_directoryInfo.FullName + "/" + TrimSlashes(Path))
            .GetFileSystemInfos()
            .Select(fileSystemInfo => new LocalFileSystem(_directoryInfo, Path + "/" + fileSystemInfo.Name) as SimpleFileSystem)
            .ToList();
    }
}