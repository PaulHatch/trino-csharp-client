using Trino.Client.Types;
using System.Numerics;

namespace Trino.Client.Test
{
    [TestClass]
    public class TrinoBigDecimalTests
    {
        [TestMethod]
        public void TestNegativeDecimalWithZeroIntegerPart()
        {
            var dec = new TrinoBigDecimal("-0.6");

            Assert.AreEqual(-1, dec.GetSign());
            Assert.AreEqual("-0.6", dec.ToString());
            Assert.AreEqual(-0.6m, dec.ToDecimal());
        }

        [TestMethod]
        public void TestNegativeDecimalWithNonZeroIntegerPart()
        {
            var dec = new TrinoBigDecimal("-123.456");

            Assert.AreEqual(-1, dec.GetSign());
            Assert.AreEqual("-123.456", dec.ToString());
            Assert.AreEqual(-123.456m, dec.ToDecimal());
        }

        [TestMethod]
        public void TestPositiveDecimal()
        {
            var dec = new TrinoBigDecimal("123.456");

            Assert.AreEqual(1, dec.GetSign());
            Assert.AreEqual("123.456", dec.ToString());
            Assert.AreEqual(123.456m, dec.ToDecimal());
        }

        [TestMethod]
        public void TestNegativeDecimalEquality()
        {
            var a = new TrinoBigDecimal("-0.6");
            var b = new TrinoBigDecimal("-0.6");
            var c = new TrinoBigDecimal("0.6");

            Assert.AreEqual(a, b);
            Assert.AreNotEqual(a, c);
        }

        [TestMethod]
        public void TestBigIntegerConstructorWithNegativeInteger()
        {
            var dec = new TrinoBigDecimal(new BigInteger(-5), new BigInteger(25), 2);

            Assert.AreEqual(-1, dec.GetSign());
            Assert.AreEqual("-5.25", dec.ToString());
            Assert.AreEqual(-5.25m, dec.ToDecimal());
        }

        [TestMethod]
        public void TestGetIntegerPartPreservesSign()
        {
            var negative = new TrinoBigDecimal("-123.456");
            var positive = new TrinoBigDecimal("123.456");

            Assert.AreEqual(new BigInteger(-123), negative.GetIntegerPart());
            Assert.AreEqual(new BigInteger(123), positive.GetIntegerPart());
        }
    }
}