from datetime import datetime, timedelta
from contextlib import contextmanager
import locale
import threading
from sys import platform
import pytz

import pandas as pd


LOCALE_LOCK = threading.Lock()
DATE_FORMAT = "%Y-%m-%d"


def clean_date(text, fmt, lang=None, loc="", minus_days=0):
    """Extract a date from a `text`.
    
    The date from text is extracted using locale `loc`. Alternatively, you can provide language `lang` instead.

    By default, system default locale is used.

    Args:
        text (str): Input text.
        fmt (str, optional): Text format. More details at
                             https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes. 
        lang (str, optional): Language two-letter code, e.g. 'da' (dansk). If given, `loc` will be ignored and redefined
                                based on `lang`. Defaults to None.
        loc (str, optional): Locale, e.g es_ES. Get list of available locales with `locale.locale_alias` or
                                `locale.windows_locale` in windows. Defaults to "" (system default).
        minus_days (int, optional): Number of days to subtract. Defaults to 0.

    Returns:
        str: Extracted date in format %Y-%m-%d
    """
    # If lang is given, map language to a locale
    if lang is not None:
        if lang in locale.locale_alias:
            loc = locale.locale_alias[lang]
    if platform == "win32":
        if loc is not None:
            loc = loc.replace("_", "-")
    # Thread-safe extract date
    with _setlocale(loc):
        return (
            datetime.strptime(text, fmt) - timedelta(days=minus_days)
        ).strftime(DATE_FORMAT)


def localdatenow(tz=None):
    if tz is None:
        tz = "utc"
    return localdate(tz, 0)


def localdate(tz, hour_limit=None, date_format=None):
    """Get local date.

    By default, gets date prior to execution.

    Args:
        tz (str, optional): Timezone name.
        hour_limit (int, optional): If local time hour is lower than this, returned date is previous day.
                                    Defaults to None.
        date_format (str, optional): Format of output datetime. Uses default YYYY-mm-dd.
    """
    tz = pytz.timezone(tz)
    local_time = datetime.now(tz=tz)
    if (hour_limit is None) or (local_time.hour < hour_limit):
        local_time = local_time - timedelta(days=1)
    if date_format is None:
        date_format = DATE_FORMAT
    return local_time.strftime(date_format)


def clean_date_series(ds: pd.Series, format_input: str, format_output: str = None) -> pd.Series:
    if format_output is None:
        format_output = DATE_FORMAT
    return pd.to_datetime(ds, format=format_input).dt.strftime(format_output)


@contextmanager
def _setlocale(name):
    # REF: https://stackoverflow.com/questions/18593661/how-do-i-strftime-a-date-object-in-a-different-locale
    with LOCALE_LOCK:
        saved = locale.setlocale(locale.LC_TIME, "")
        try:
            yield locale.setlocale(locale.LC_TIME, name)
        finally:
            locale.setlocale(locale.LC_TIME, saved)
