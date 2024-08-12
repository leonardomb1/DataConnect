using DataConnect.Controller;

namespace DataConnect;

public class Program
{
    public static void Main(string[] args)
    {
        if (args.Contains("--help") || args.Contains("-h")) {
            ShowHelp();
            return;
        }

        if (args.Contains("--version") || args.Contains("-v")) {
            ShowVersion();
            return;
        }

        bool isCmd = 
                    args.Contains("--environment") || 
                    args.Contains("-e") || 
                    args.Length > 0;

        Run(isCmd, args);
    }

    private static void Run(bool isCmd, string[] args)
    {
        int port = 0;
        string connection = "";
        string database = "";

        if (!isCmd) {
            string[] envs = {
                "PORT_TO_USE", 
                "DW_CONNECTIONSTRING",
                "EXPORT_DATABASE",
            };

            Dictionary<string, string?> config = envs.ToDictionary(
                        env => env,
                        Environment.GetEnvironmentVariable
                    );

            if (config.Any(variable => variable.Value is null)) {
                throw new Exception("Environment variable not configured!");
            }

            port = int.Parse(config[envs[0]]!);
            connection = config[envs[1]]!;
        } else {
            if (args.Length < 3) {
                Console.WriteLine(
                    "Expected:  -e  <port> <connection>"
                );
                return;
            }
            
            port = int.Parse(args[1]);
            connection = args[2];
            database = args[3];
        }

        using var server = new Server(port, connection, database);
        server.Start();
        Console.Read();
    }

    private static void ShowHelp() 
    {
        ShowSignature();
        Console.WriteLine(
            "Usage: DataConnect [options]\n" +
            "Options:\n" +
            "   -h --help      Show this help message\n" +
            "   -v --version   Show version information\n" +
            "   -e --environment    <port> <connection>   Use configuration variables\n\n" +
            "Example:\n" +
            "   DataConnect -e <port> <connection>"
        );
    }

    private static void ShowVersion() 
    {
        ShowSignature();
        Console.WriteLine("DataConnect version 1.0.0");
    }

    private static void ShowSignature() 
    {
        Console.WriteLine(
            "Developed by Leonardo M. Baptista\n" +
            "Licensed under the MIT License. see LICENSE file for details.\n"
        );
    }
}