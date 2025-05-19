using System;

namespace ParquetSharp.Dataset.Filter;

internal static class TimeUtils
{
    public static long ToPrimitiveValue(DateTime dateTime, Apache.Arrow.Types.TimeUnit unit)
    {
        var ticks = (dateTime - ArrowEpoch).Ticks;

        return unit switch
        {
            Apache.Arrow.Types.TimeUnit.Second => ticks / TimeSpan.TicksPerSecond,
            Apache.Arrow.Types.TimeUnit.Millisecond => ticks / TimeSpan.TicksPerMillisecond,
            Apache.Arrow.Types.TimeUnit.Microsecond => ticks / (TimeSpan.TicksPerMillisecond / 1000),
            Apache.Arrow.Types.TimeUnit.Nanosecond => checked(ticks * 100),
            _ => throw new ArgumentOutOfRangeException(nameof(unit), unit, "Invalid timestamp unit")
        };
    }

    public static int DateToDayNumber(DateOnly date)
    {
        return date.DayNumber - ArrowDateEpoch.DayNumber;
    }

    private static readonly DateTime ArrowEpoch = new DateTime(1970, 1, 1, 0, 0, 0, 0);
    private static readonly DateOnly ArrowDateEpoch = new DateOnly(1970, 1, 1);
}
