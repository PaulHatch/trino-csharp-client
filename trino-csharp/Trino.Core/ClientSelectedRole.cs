using System;
using Trino.Core.Utils;

namespace Trino.Core;

/// <summary>
/// Supports Trino ClientSelectedRole header.
/// </summary>
public class ClientSelectedRole
{
    public enum Type
    {
        ROLE, ALL, NONE
    }

    public Type RoleType { get; }
    public string Role { get; }

    public ClientSelectedRole(Type roleType, string role)
    {
        RoleType = roleType.IsNullArgument("roletype");
        Role = role.IsNullArgument("role");
    }

    public override bool Equals(object? o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || GetType() != o.GetType())
        {
            return false;
        }
        var that = (ClientSelectedRole)o;
        return RoleType == that.RoleType && Role == that.Role;
    }

    public override int GetHashCode()
    {
        return RoleType.GetHashCode() ^ Role.GetHashCode();
    }

    public ClientSelectedRole Clone()
    {
        return new ClientSelectedRole(RoleType, Role);
    }

    public override string ToString() => $"{RoleType}:{{{Role}}}";
}