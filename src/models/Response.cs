namespace DataConnect.Models;

public class Response : IDisposable
{
    private bool _disposed;
    public required int Status {get; set;}
    public required bool Error {get; set;}
    public required string Message {get; set;}
    public List<string>? Options {get; set;}

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

    ~Response()
    {
        Dispose(false);
    }
}
