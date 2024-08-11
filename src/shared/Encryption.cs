using System.Security.Cryptography;
using System.Text;

namespace DataConnect.Shared;

public static class Encryption
{
    public static string Sha256(string data)
    {
        byte[] bytes = SHA256.HashData(Encoding.UTF8.GetBytes(data));
        StringBuilder builder = new();

        for (int i = 0; i < bytes.Length; i++)
        {
            builder.Append(bytes[i].ToString("x2"));
        }
        
        return builder.ToString();
    }
}