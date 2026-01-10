using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Trino.Core.Auth
{
    public class TrinoOauthClientSecretAuth : ITrinoAuth
    {
        public string TokenEndpoint { get; set; }
        public string ClientId { get; set; }
        public string Scope { get; set; }

        public string ClientSecret { private get; set; }
        private string accessToken;
        private DateTime tokenExpiry;

        public TrinoOauthClientSecretAuth()
        {
            // parameterless constructor required for connection string use
        }

        public TrinoOauthClientSecretAuth(string tokenEndpoint, string clientId, string clientSecret, string scope)
        {
            TokenEndpoint = tokenEndpoint;
            ClientId = clientId;
            ClientSecret = clientSecret;
            Scope = scope;
        }

        public void AuthorizeAndValidate()
        {
            if (string.IsNullOrEmpty(TokenEndpoint) || string.IsNullOrEmpty(ClientId) ||
                string.IsNullOrEmpty(ClientSecret) || string.IsNullOrEmpty(Scope))
            {
                throw new InvalidOperationException("OAuth2 configuration is missing required properties.");
            }

            var tokenResponse = GetTokenAsync(TokenEndpoint, ClientId, ClientSecret, Scope).Result;
            accessToken = tokenResponse.AccessToken;
            tokenExpiry = DateTime.UtcNow.AddSeconds(tokenResponse.ExpiresIn);
        }

        public void AddCredentialToRequest(HttpRequestMessage httpRequestMessage)
        {
            if (string.IsNullOrEmpty(accessToken) || DateTime.UtcNow >= tokenExpiry)
            {
                AuthorizeAndValidate();
            }

            httpRequestMessage.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", accessToken);
        }

        private async Task<TokenResponse> GetTokenAsync(string tokenEndpoint, string clientId, string clientSecret, string scope)
        {
            using (var httpClient = new HttpClient())
            {
                var request = new HttpRequestMessage(HttpMethod.Post, tokenEndpoint)
                {
                    Content = new FormUrlEncodedContent([
                        new KeyValuePair<string, string>("client_id", clientId),
                        new KeyValuePair<string, string>("client_secret", clientSecret),
                        new KeyValuePair<string, string>("grant_type", "client_credentials"),
                        new KeyValuePair<string, string>("scope", scope)
                    ])
                };

                var response = await httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var content = await response.Content.ReadAsStringAsync();
                return JsonSerializer.Deserialize<TokenResponse>(content);
            }
        }

        private class TokenResponse
        {
            [JsonPropertyName("access_token")]
            public string AccessToken { get; set; }

            [JsonPropertyName("expires_in")]
            public int ExpiresIn { get; set; }
        }
    }
}
