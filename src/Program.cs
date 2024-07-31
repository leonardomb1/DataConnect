using DataConnect.Controller;

namespace DataConnect;

public class Program
{
    private int _packetSize;
    private readonly string _connection;
    private readonly int _port;
    public Program()
    {
        var configVariables = new Dictionary<string, string>
        {
            { "PACKET_SIZE", Environment.GetEnvironmentVariable("PACKET_SIZE")?? "n/a" },
            { "PORT_TO_USE", Environment.GetEnvironmentVariable("PORT_TO_USE")?? "n/a" },
            { "DW_CONNECTIONSTRING", Environment.GetEnvironmentVariable("DW_CONNECTIONSTRING")?? "n/a" }
        };

        var anyConfigNotSet = configVariables.Any(variable => variable.Value == "n/a");

        if (anyConfigNotSet)
        {
            throw new Exception("Environment variable not configured!");
        }

        _packetSize = int.Parse(configVariables["PACKET_SIZE"]);
        _port = int.Parse(configVariables["PORT_TO_USE"]);
        _connection = configVariables["DW_CONNECTIONSTRING"];
    }

    public void Run()
    {
        using var controller = new Route(_port, _connection);
        controller.Start();
        Console.Read();
    }

    public static void Main(string[] args)
    {
        new Program().Run();
    }
}