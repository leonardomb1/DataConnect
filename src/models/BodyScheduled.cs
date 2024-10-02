namespace DataConnect.Models;

public class BodyScheduled : IDisposable
{
    private bool _disposed;
    public required int SystemId {get; set;}
    public required int ScheduleId {get; set;}
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

    ~BodyScheduled()
    {
        Dispose(false);
    } 
}