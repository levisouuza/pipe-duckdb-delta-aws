from datetime import datetime, timedelta

import pytz


def get_current_date():
    sp_timezone = pytz.timezone("America/Sao_Paulo")
    current_date = datetime.now(sp_timezone)
    return current_date.strftime("%Y%m%d")


def get_yesterday_date():
    sp_timezone = pytz.timezone("America/Sao_Paulo")
    current_date = datetime.now(sp_timezone)
    yesterday_date = current_date - timedelta(days=1)
    return yesterday_date.strftime("%Y%m%d")
