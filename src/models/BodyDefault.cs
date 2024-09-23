namespace DataConnect.Models;

public class BodyDefault : IDisposable
{
    private bool _disposed;
    public required string ConnectionInfo {get; set;}
    public required string DestinationTableName {get; set;}
    public required string SysName {get; set;}
    public required string[] Options {get; set;}

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    private void Dispose(bool disposing)
    {
        if (!_disposed) {
            return;
        }

        if (disposing) {
            //
        }

        _disposed = true;
    }

    ~BodyDefault()
    {
        Dispose(false);
    } 
}