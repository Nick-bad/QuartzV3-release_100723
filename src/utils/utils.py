import datetime as dt
from dateutil import tz
import logging
import datetime as dt
from src.utils.emails_interface import MailGunEmail
from src.utils.config import *

logger = logging.getLogger('main_logger')


def localize(d: dt.datetime) -> dt.datetime:
    '''
    Returns UTC datetime to local
    :param dt:
    :return:
    '''
    if not isinstance(d, dt.datetime):
        d = dt.datetime(d.year, d.month, d.day)
    return d.replace(tzinfo=tz.tzutc()).astimezone(tz.tzlocal())


def exception_handler(func):
    def inner_function(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            import traceback
            e = traceback.format_exc()
            logger.error(e)
            if is_exception_to_email(e):
                mg = MailGunEmail(env='Unknown env')
                error_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M')
                mg.send_text_email('Error reported by generic exception_handler',
                                            f'error time: {error_time} \n error desc: {e}')
    return inner_function


def is_exception_to_email(exception) -> bool:
    email = True
    for e in NO_EMAIL_EXCEPTIONS:
        if e in exception:
            email = False
            break
    return email


def convert_to_excel_date(date):
    temp = dt.datetime(1899, 12, 30)  # Note, not 31st Dec but 30th!
    delta = date - temp
    return int(delta.days)

def rolling_mean(df, groupby_col, feature, window):
    return df.groupby(groupby_col)[feature].rolling(window=window, min_periods=1).mean().reset_index(0, drop=True)

def rolling_max(df, groupby_col, feature, window):
    return df.groupby(groupby_col)[feature].rolling(window=window, min_periods=1).max().reset_index(0, drop=True)

def rolling_min(df, groupby_col, feature, window):
    return df.groupby(groupby_col)[feature].rolling(window=window, min_periods=1).min().reset_index(0, drop=True)

def rolling_sum(df, groupby_col, feature, window):
    return df.groupby(groupby_col)[feature].rolling(window=window, min_periods=1).sum().reset_index(0, drop=True)

def cum_sum(df, groupby_col, feature):
    return df.groupby(groupby_col)[feature].cumsum()

def cum_count(df, groupby_col, feature):
    return df.groupby(groupby_col)[feature].cumcount() + 1

def cum_mean(df, groupby_col, feature):
    return df.groupby(groupby_col)[feature].cumsum() / (df.groupby(groupby_col)[feature].cumcount() + 1)

def rolling_count(df, groupby_col, feature, window):
    return df.groupby(groupby_col)[feature].rolling(window=window, min_periods=1).count().reset_index(0, drop=True)

def rolling_mean_ewm(df, groupby_col, feature, halflife):
    return df.groupby(groupby_col).apply(lambda x: x[feature].ewm(halflife=halflife, adjust=True, ignore_na=True).mean()).reset_index(0,drop=True).T

def shift_group_by(df, groupby_feature, feature, lag=1):
    return df.groupby(groupby_feature)[feature].shift(lag)

def clean_dog_name(dog_name):
    dog_name = dog_name.replace('(Res)', '')
    dog_name = dog_name.replace('(RES)', '')
    dog_name = dog_name.strip()
    return dog_name