using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EFCache.Redis
{
    public static class Guard
    {
        public static void GuardAgainstLessEqualZero(this int value, string parameterName)
        {
            if (value <= 0)
            {
                throw new ArgumentOutOfRangeException(parameterName);
            }
        }
        public static void ForPrecedesDate(this DateTime value, DateTime dateToPrecede, string parameterName)
        {
            if (value >= dateToPrecede)
            {
                throw new ArgumentOutOfRangeException(parameterName);
            }
        }
        public static void GuardAgainstNullOrEmpty(this string value, string parameterName)
        {
            if (string.IsNullOrEmpty(value))
            {
                throw new ArgumentOutOfRangeException(parameterName);
            }
        }

        public static void GuardAgainstNull<T>(this T value, string parameterName, string extraInformation)
        {
            GuardAgainstValueType<T>(parameterName);

            if (value == null)
            {
                throw new ArgumentNullException(parameterName, extraInformation);
            }
        }

        public static void GuardAgainstNull<T>(this T value, string parameterName)
        {
            GuardAgainstValueType<T>(parameterName);

            if (value == null)
            {
                throw new ArgumentNullException(parameterName);
            }
        }
        public static void GuardAgainstEmptyCollection<T>(this IEnumerable<T> value, string parameterName)
        {
            if (value == null)
            {
                throw new ArgumentNullException(parameterName);
            }

            if (!value.Any())
            {
                throw new ArgumentException("The IEnumerable should have at least one element.", parameterName);
            }

        }
        private static void GuardAgainstValueType<T>(string parameterName)
        {
            if (typeof(T).IsValueType)
            {
                throw new ArgumentException("parameter should be reference type, not value type", parameterName);
            }
        }
    }
}
