using System.Net;
using DataConnect.Shared;

namespace DataConnect.Controller;

public class Controller
{
    private readonly HttpListener _listener;

    private readonly int _port;

    public Controller(int port)
    {
        _listener = new HttpListener();
        _listener.Prefixes.Add($"http://*:{port}/api/execute");
        _port = port;
    }

    public void Run()
    {
        _listener.Start();
        Console.WriteLine($"Escutando em http://*:{_port}/api/execute ...");
    }

    public void Stop()
    {
        _listener.Stop();
    }

    private void Receive()
    {
        _listener.BeginGetContext(new AsyncCallback(ListenerCallback), _listener);
    }

    private void ListenerCallback(IAsyncResult result)
    {
        if (_listener.IsListening)
        {
            var ctx = _listener.EndGetContext(result);
            var req = ctx.Request;
            
            new Log().Out($"{req.HttpMethod} Request received, from {req.UserAgent} by {req.UserHostName} in {req.UserHostAddress} with {req.InputStream}");

            Receive();
        }
    }
}