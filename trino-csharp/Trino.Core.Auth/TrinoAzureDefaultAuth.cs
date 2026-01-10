using System.Net.Http;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Core;
using System;

namespace Trino.Core.Auth
{
    public class TrinoAzureDefaultAuth : ITrinoAuth
    {
        private readonly DefaultAzureCredential credential;
        private AccessToken accessToken;
        private readonly string scope;

        public TrinoAzureDefaultAuth(string scope)
        {
            credential = new DefaultAzureCredential();
            this.scope = scope;
            accessToken = GetTokenAsync().GetAwaiter().GetResult();
        }

        public void AuthorizeAndValidate()
        {
            // This method can be used to trigger manual authorization if needed.
            // For example, you could prompt the user to login or refresh the token.
            // Here, we'll just fetch the token.
            accessToken = GetTokenAsync().GetAwaiter().GetResult();
        }

        public void AddCredentialToRequest(HttpRequestMessage httpRequestMessage)
        {
            if (accessToken.ExpiresOn <= DateTimeOffset.UtcNow)
            {
                accessToken = GetTokenAsync().GetAwaiter().GetResult();
            }

            httpRequestMessage.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", accessToken.Token);
        }

        private async Task<AccessToken> GetTokenAsync()
        {
            return await credential.GetTokenAsync(new TokenRequestContext([scope]));
        }
    }
}
