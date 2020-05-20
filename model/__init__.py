import logging
from datetime import datetime

logger = logging.getLogger(__name__)

today = datetime.now()
today_str = today.strftime('%Y-%m-%d')


def get_percent(numerator, denominator):
    return int((numerator / denominator * 100.0) + 0.5)


