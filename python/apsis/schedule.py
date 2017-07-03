import bisect
from   cron import *

from   .lib import *

#-------------------------------------------------------------------------------

class DailySchedule:

    def __init__(self, tz, calendar, daytimes):
        self.__tz       = TimeZone(tz)
        self.__calendar = calendar
        self.__daytimes = tuple(sorted( Daytime(t) for t in daytimes ))


    def __call__(self, start):
        """
        Generates scheduled times starting not before `start`.
        """
        start = Time(start)

        local = to_local(start, self.__tz)
        if local.date in self.__calendar:
            date = local.date
            # Find the next daytime.
            for i, daytime in enumerate(self.__daytimes):
                if local.daytime <= daytime:
                    break
            else:
                # All daytimes have passed for this date.
                date = self.__calendar.shift(date, 1)
                i = 0
        else:
            # Start at the beginning of the next date.
            date = self.__calendar.next(local.date)
            i = 0

        # Now generate.
        while True:
            yield from_local((date, self.__daytimes[i]), self.__tz)
            i += 1
            if i == len(self.__daytimes):
                # On to the next day.
                date = self.__calendar.shift(date, 1)
                i = 0



class ExplicitSchedule:

    def __init__(self, times):
        self.__times = tuple(sorted( Time(t) for t in times ))


    def __call__(self, start):
        start = Time(start)
        i = bisect.bisect_left(self.__times, start)
        return iter(self.__times[i :])



class CrontabSchedule:
    # FIXME: This could be made much more efficient.
    # FIXME: Crontab accepts 0 = 7 = Sunday, but we only accept 0.

    def __init__(
            self, tz, 
            minute  =[Interval(0, 60)],
            hour    =[Interval(0, 24)], 
            day     =[Interval(0, 32)], 
            month   =[Interval(1, 13)],
            weekday =[Interval(0,  7)]
    ):
        normalize = lambda ss: tuple(
            s if isinstance(s, Interval) else Interval(s, s + 1) 
            for s in ss
        )

        self.__tz       = TimeZone(tz)
        self.__minute   = normalize(minute)
        self.__hour     = normalize(hour)
        self.__day      = normalize(day)
        self.__month    = normalize(month)
        self.__weekday  = normalize(weekday)


    def __str__(self):
        return " ".join(
            ",".join(
                     str(i.start) if i.start == i.stop - 1
                else "{}-{}".format(i.start, i.stop - 1)
                for i in ss
            )
            for ss in (
                self.__minute, self.__hour, self.__day, self.__month, 
                self.__weekday
            )
        )


    def __call__(self, start):
        time = Time(start)
        # Advance to the next round minute.
        # FIXME: Add this to cron.
        off = (Time.MIN + 60).offset - Time.MIN.offset
        time = Time.from_offset((Time(start).offset + off - 1) // off * off)

        check = lambda v, ss: any( v in s for s in ss )

        while True:
            date, daytime = time @ self.__tz
            if (
                    check(daytime.minute, self.__minute)
                and check(daytime.hour, self.__hour)
                and check(date.month, self.__month)
                and (
                       check(date.day, self.__day)
                    or check((date.weekday - Sun + 7) % 7, self.__weekday)
                )
            ):
                yield time
            time += 60



