namespace DataConnect.Features.Authentication.Services;

public interface IAuthenticationService
{
    string GenerateExpectedToken(string authSecret);
    bool ValidateToken(string providedToken, string authSecret);
}