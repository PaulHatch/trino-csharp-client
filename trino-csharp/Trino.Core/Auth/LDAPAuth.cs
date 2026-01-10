using System;

namespace Trino.Core.Auth;

public class LdapAuth : BasicAuth
{
    public LdapAuth()
    {
    }

    public override void AuthorizeAndValidate()
    {
        if (string.IsNullOrEmpty(User) || string.IsNullOrEmpty(Password))
        {
            throw new ArgumentException("LDAPAuth: username or password property is null or empty");
        }
    }
}