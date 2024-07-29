using System.Security.Cryptography;

namespace DataConnect.Shared;

public static class Cryptography
{
    public static async Task<byte[]> EncryptAES(string sendData, byte[] key, byte[] iv)
    {
        using var aes = Aes.Create();
        aes.Key = key;
        aes.IV = iv;

        ICryptoTransform transform = aes.CreateEncryptor(aes.Key, aes.IV);

        using var dataStream = new MemoryStream();
        using var cryptoStream = new CryptoStream(dataStream, transform, CryptoStreamMode.Write);
        using var writer = new StreamWriter(cryptoStream);
        await writer.WriteAsync(sendData);

        return dataStream.ToArray(); 
    }
}
