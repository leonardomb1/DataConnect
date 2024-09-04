using DataConnect.Controller;
using DataConnect.Shared;

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
        int threadTimeout = 0;
        int threadPagination = 0;
        string connection = "";
        string database = "";

        if (!isCmd) {
            string[] envs = {
                "PORT_TO_USE", 
                "DW_CONNECTIONSTRING",
                "EXPORT_DATABASE",
                "THREAD_PAGINATION",
                "THREAD_TIMEOUT",
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
            database = config[envs[2]]!;
            threadPagination = int.Parse(config[envs[3]]!);
            threadTimeout = int.Parse(config[envs[4]]!);
        } else {
            if (args.Length < 3) {
                Console.WriteLine(
                    "Expected:  -e  <port> <connection> <database> <thread timeout>"
                );
                return;
            }
            
            port = int.Parse(args[1]);
            connection = args[2];
            database = args[3];
            threadPagination = int.Parse(args[4]);
            threadTimeout = int.Parse(args[5]);
        }

        using var server = new Server(port, connection, database, threadPagination, threadTimeout);
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
            "   DataConnect -e <port> <connection> <database> <threads> <thread timeout>"
        );
    }

    private static void ShowVersion() 
    {
        ShowSignature();
        Console.WriteLine($"DataConnect version ${Constants.ProgramVersion}");
    }

    private static void ShowSignature() 
    {
        Console.WriteLine(
            "Developed by Leonardo M. Baptista\n" +
            "Licensed under the MIT License. see LICENSE file for details.\n"
        );
    }
}