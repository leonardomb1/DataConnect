namespace DataConnect.Models;

public class Error : IDisposable
{
    private bool _disposed;
    public required string ExceptionMessage {get; set;}
    public bool? IsPartialSuccess {get; set;}
    public int? ErrorCount {get; set;}

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


    ~Error()
    {
        Dispose(false);
    } 
}