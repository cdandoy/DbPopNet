using System.Collections.ObjectModel;
using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using DbPop.DbPopNet.Db;
using DbPop.DbPopNet.Fs;

namespace DbPop.DbPopNet.Upload;

public sealed class Populator : IDisposable
{
    private readonly Database _database;
    private readonly Dictionary<string, Dataset> _datasetsByName;
    private readonly Dictionary<TableName, Table> _tablesByName;
    private static readonly Lazy<Populator> LazyUploader = new(() => Builder().Build());

    private Populator(Database database, Dictionary<string, Dataset> datasetsByName, Dictionary<TableName, Table> tablesByName)
    {
        _database = database;
        _datasetsByName = datasetsByName;
        _tablesByName = tablesByName;
    }

    public void Dispose()
    {
        if (LazyUploader.IsValueCreated && LazyUploader.Value == this) return; // Do not close the singleton
        _database.Dispose();
    }

    public static Populator Instance => LazyUploader.Value;

    public static Builder Builder()
    {
        return new Builder();
    }

    internal static Populator Build(Builder builder)
    {
        var sqlConnection = DbPop.CreateConnection(
            builder.Env,
            builder.ConnectionString,
            builder.Username,
            builder.Password
        );

        var database = Database.CreateDatabase(sqlConnection);

        var simpleFileSystem = builder.SimpleFileSystem();
        var datasetsByName = Datasets(simpleFileSystem);
        if (datasetsByName.Count == 0) throw new Exception("No datasets found in " + simpleFileSystem);

        var datasetTableNames = datasetsByName
            .Select(pair => pair.Value)
            .SelectMany(dataset => dataset.DataFiles())
            .Select(dataFile => dataFile.TableName)
            .ToHashSet();
        var databaseTables = database.Tables(datasetTableNames);
        ValidateAllTablesExist(datasetsByName, datasetTableNames, databaseTables);
        var tablesByName = new Dictionary<TableName, Table>();
        databaseTables.ForEach(table => tablesByName[table.TableName] = table);

        return new Populator(database, datasetsByName, tablesByName);
    }

    private static void ValidateAllTablesExist(Dictionary<string, Dataset> allDatasets, HashSet<TableName> datasetTableNames, List<Table> databaseTables)
    {
        var databaseTableNames = databaseTables
            .Select(table => table.TableName)
            .ToHashSet();
        var missingTables = datasetTableNames
            .Where(tableName => !databaseTableNames.Contains(tableName))
            .ToList();

        if (missingTables.Count > 0)
        {
            var badDataFile = allDatasets
                .SelectMany(pair => pair.Value.DataFiles())
                .First(dataFile => missingTables.Contains(dataFile.TableName));
            if (badDataFile == null) throw new Exception();
            throw new Exception($"Table ${badDataFile.TableName.ToQualifiedName()} does not exist for this data file ${badDataFile.SimpleFileSystem}");
        }
    }

    private static Dictionary<string, Dataset> Datasets(SimpleFileSystem simpleFileSystem)
    {
        var datasets = new Dictionary<string, Dataset>();

        var datasetFiles = simpleFileSystem.List();
        if (datasetFiles.Count == 0) throw new Exception("Invalid path " + simpleFileSystem);
        foreach (var datasetFile in datasetFiles)
        {
            var catalogFiles = datasetFile.List();
            var dataFiles = new List<DataFile>();
            foreach (var catalogFile in catalogFiles)
            {
                var catalog = catalogFile.Name();
                var schemaFiles = catalogFile.List();
                foreach (var schemaFile in schemaFiles)
                {
                    var schema = schemaFile.Name();
                    var tableFiles = schemaFile.List();
                    foreach (var tableFile in tableFiles)
                    {
                        var tableFileName = tableFile.Name();
                        if (tableFileName.EndsWith(".csv"))
                        {
                            var table = tableFileName[..^4];
                            dataFiles.Add(
                                new DataFile(
                                    tableFile,
                                    new TableName(catalog, schema, table)
                                )
                            );
                        }
                    }
                }
            }

            if (dataFiles.Count > 0)
            {
                var datasetName = datasetFile.Name();
                datasets[datasetName] = new Dataset(datasetName, dataFiles);
            }
        }

        return datasets;
    }

    public int Load(params string[] datasets)
    {
        return Load(datasets.ToList());
    }

    public int Load(List<string> datasets)
    {
        var transaction = _database.GetConnection().BeginTransaction();
        var rowCount = 0;
        var loadedTables = LoadedTables(datasets);
        var preparationStrategy = _database.CreateDatabasePreparationStrategy(_tablesByName.Values);
        preparationStrategy.BeforeInserts();
        try
        {
            foreach (var datasetName in datasets)
            {
                if (!_datasetsByName.ContainsKey(datasetName))
                    throw new Exception($"Dataset not found : ${datasetName}");
                var dataset = _datasetsByName[datasetName];
                rowCount += LoadDataset(dataset);
            }
        }
        finally
        {
            preparationStrategy.AfterInserts();
        }

        transaction.Commit();
        return rowCount;
    }

    private int LoadDataset(Dataset dataset)
    {
        var rowCount = 0;
        foreach (var dataFile in dataset.DataFiles())
        {
            rowCount += LoadDataFile(dataFile);
        }

        return rowCount;
    }

    private int LoadDataFile(DataFile dataFile)
    {
        var tableName = dataFile.TableName;
        var table = _tablesByName[tableName];

        var csvConfiguration = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = true
        };
        using var csvReader = new CsvReader(dataFile.TextReader(), csvConfiguration);
        return InsertRows(table, csvReader);
    }

    private int InsertRows(Table table, CsvReader csvReader)
    {
        var headerNames = csvReader.HeaderRecord;
        if (headerNames == null) return 0;
        var dataFileHeaders = headerNames.Select(it => new DataFileHeader(it)).ToList();
        using var databaseInserter = _database.CreateInserter(table, dataFileHeaders);
        var count = 0;
        while (csvReader.Read())
        {
            databaseInserter.Insert(csvReader);
            count++;
        }

        return count;
    }

    private ISet<Table> LoadedTables(List<string> datasets)
    {
        return _datasetsByName.Values
            .Where(it => datasets.Contains(it.Name()))
            .Select(it => it.DataFiles())
            .SelectMany(it => it)
            .Where(it => _tablesByName.ContainsKey(it.TableName))
            .Select(it => _tablesByName[it.TableName])
            .ToHashSet();
    }
}

internal class Dataset
{
    private readonly string _name;
    private readonly List<DataFile> _dataFiles;

    public Dataset(string name, List<DataFile> dataFiles)
    {
        _name = name;
        _dataFiles = dataFiles;
    }

    public string Name() => _name;
    public ReadOnlyCollection<DataFile> DataFiles() => _dataFiles.AsReadOnly();
}

internal class DataFile
{
    public DataFile(SimpleFileSystem simpleFileSystem, TableName tableName)
    {
        SimpleFileSystem = simpleFileSystem;
        TableName = tableName;
    }

    public SimpleFileSystem SimpleFileSystem { get; }
    public TableName TableName { get; }

    public TextReader TextReader()
    {
        return SimpleFileSystem.TextReader();
    }
}