using System.Security.Cryptography;
using System.Text;

namespace DataConnect.Core.Common;

public static class Encryption
{
    public static string Sha256(string data)
    {
        byte[] bytes = SHA256.HashData(Encoding.UTF8.GetBytes(data));
        return GetStringFromByteArray(bytes);
    }

    public static string SymmetricEncryptAES256(string input, string authSecret)
    {
        byte[] keyBytes = GetByteArrayFromHash(authSecret);
        byte[] inputBytes = Encoding.UTF8.GetBytes(input);

        using Aes aes = Aes.Create();
        aes.KeySize = 256;
        aes.BlockSize = 128;
        aes.GenerateIV();
        aes.Key = keyBytes;

        using MemoryStream mem = new();
        using CryptoStream crypto = new(mem, aes.CreateEncryptor(), CryptoStreamMode.Write);
        crypto.Write(inputBytes, 0, inputBytes.Length);
        crypto.FlushFinalBlock();

        byte[] result = mem.ToArray();
        string stringResult = Convert.ToBase64String(aes.IV.Concat(result).ToArray());

        return stringResult;
    }

    public static string SymmetricDecryptAES256(string input, string authSecret)
    {
        byte[] fullCipher = Convert.FromBase64String(input);
        byte[] iv = fullCipher.Take(16).ToArray();
        byte[] cipherBytes = fullCipher.Skip(16).ToArray();
        byte[] keyBytes = GetByteArrayFromHash(authSecret);

        using Aes aes = Aes.Create();
        aes.KeySize = 256;
        aes.BlockSize = 128;
        aes.Key = keyBytes;
        aes.IV = iv;

        using MemoryStream mem = new(cipherBytes);
        using CryptoStream crypto = new(mem, aes.CreateDecryptor(), CryptoStreamMode.Read);
        using StreamReader stream = new(crypto);

        string result = stream.ReadToEnd();

        return result;
    }

    private static byte[] GetByteArrayFromHash(string input)
    {
        string hashString = Sha256(input);

        byte[] hashBytes = new byte[32];
        for (int i = 0; i < hashBytes.Length; i++)
        {
            hashBytes[i] = Convert.ToByte(hashString.Substring(i * 2, 2), 16);
        }
        return hashBytes;
    }

    private static string GetStringFromByteArray(byte[] bytes) 
    {
        StringBuilder builder = new();

        for (int i = 0; i < bytes.Length; i++)
        {
            builder.Append(bytes[i].ToString("x2"));
        }
        return builder.ToString();
    }
}
