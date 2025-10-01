using DataConnect.Core.Common;

namespace DataConnect.Features.Authentication.Services;

public class AuthenticationService : IAuthenticationService
{
    public string GenerateExpectedToken(string authSecret)
    {
        return Encryption.Sha256(authSecret + DateTime.Today.ToString("dd/MM/yyyy"));
    }

    public bool ValidateToken(string providedToken, string authSecret)
    {
        var expectedToken = GenerateExpectedToken(authSecret);
        return string.Equals(providedToken, expectedToken, StringComparison.Ordinal);
    }
}