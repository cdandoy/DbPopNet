namespace DbPop.DbPopNet.Upload;

public class DataFileHeader
{
    public string ColumnName { get; }
    public bool Binary { get; }
    public DataFileHeader(string header)
    {
        var optBinary = false;
        while (true)
        {
            var i = header.LastIndexOf('*');
            if (i == -1) break;
            var opt = header[(i + 1)..];
            if ("b64" == opt) optBinary = true;
            header = header[..i];
        }

        Binary = optBinary;
        ColumnName = header;
    }

}