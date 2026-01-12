using System;
using System.Numerics;

namespace Trino.Core.Types;

public struct TrinoBigDecimal
{
    private BigInteger integerPart;
    private BigInteger fractionalPart;
    private int scale;
    private int sign; // store explicit sign to preserve "-0.xxx" cases

    public TrinoBigDecimal(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("Invalid decimal string.");
        }

        value = value.Trim();

        // Detect sign explicitly
        sign = value.StartsWith("-", StringComparison.Ordinal) ? -1 : 1;
        if (value.StartsWith("+") || value.StartsWith("-"))
        {
            value = value.Substring(1);
        }

        var parts = value.Split('.');
        integerPart = BigInteger.Parse(parts[0]);
        fractionalPart = parts.Length > 1 ? BigInteger.Parse(parts[1]) : BigInteger.Zero;
        scale = parts.Length > 1 ? parts[1].Length : 0;

        Validate();
    }

    public TrinoBigDecimal(BigInteger integerPart, BigInteger fractionalPart, int scale)
    {
        sign = integerPart.Sign < 0 ? -1 : 1;
        this.integerPart = BigInteger.Abs(integerPart);
        this.fractionalPart = fractionalPart;
        this.scale = scale;
        Validate();
    }

    private void Validate()
    {
        if (fractionalPart < 0)
        {
            throw new ArgumentException("fractionalPart cannot be negative.");
        }
    }

    public override string ToString()
    {
        var core = scale > 0
            ? $"{integerPart}.{fractionalPart.ToString().PadLeft(scale, '0')}"
            : integerPart.ToString();
        return sign < 0 ? "-" + core : core;
    }

    public decimal ToDecimal()
    {
        const int maxPrecisionForDecimal = 28;

        var total = integerPart * BigInteger.Pow(10, scale) + fractionalPart;
        var result = (decimal)total / (decimal)BigInteger.Pow(10, scale);

        if (sign < 0)
        {
            result = -result;
        }

        // Validate precision
        var digits = integerPart.IsZero ? 0 : (int)Math.Floor(BigInteger.Log10(integerPart)) + 1;
        if (scale + digits > maxPrecisionForDecimal)
        {
            throw new OverflowException("The precision exceeds the allowable limit for a decimal.");
        }

        return result;
    }

    public int GetScale() => scale;

    public int GetPrecision()
    {
        var integerDigits = integerPart.IsZero ? 0 : (int)Math.Floor(BigInteger.Log10(integerPart)) + 1;
        return integerDigits + scale;
    }

    public int GetSign() => sign;

    public BigInteger GetIntegerPart() => integerPart * sign;

    public BigInteger GetFractionalPart() => fractionalPart;

    public override bool Equals(object? obj)
    {
        if (obj is TrinoBigDecimal other)
        {
            AlignScales(ref this, ref other);
            return sign == other.sign &&
                   integerPart == other.integerPart &&
                   fractionalPart == other.fractionalPart;
        }
        return false;
    }

    public override int GetHashCode()
    {
        unchecked
        {
            var hash = 17;
            hash = hash * 31 + integerPart.GetHashCode();
            hash = hash * 31 + fractionalPart.GetHashCode();
            hash = hash * 31 + scale.GetHashCode();
            hash = hash * 31 + sign.GetHashCode();
            return hash;
        }
    }

    private static void AlignScales(ref TrinoBigDecimal a, ref TrinoBigDecimal b)
    {
        if (a.scale > b.scale)
        {
            b.fractionalPart *= BigInteger.Pow(10, a.scale - b.scale);
            b.scale = a.scale;
        }
        else if (b.scale > a.scale)
        {
            a.fractionalPart *= BigInteger.Pow(10, b.scale - a.scale);
            a.scale = b.scale;
        }
    }
}