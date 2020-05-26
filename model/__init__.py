import logging
from datetime import datetime

logger = logging.getLogger(__name__)

today = datetime.now()
today_str = today.strftime('%Y-%m-%d')


def get_percent(numerator, denominator=None):
    if denominator:
        numerator /= denominator
    return '%.0f%%' % (numerator * 100.0)


