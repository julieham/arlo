from crontab import CronTab
from dateutil.utils import today

cron = CronTab()
write = False

for job in cron:
    date_today = today()
    values = str(job).split()
    if (values[2] != '*') & (values[3] != '*'):
        month, day = int(values[2]), int(values[3])
        is_past = (date_today.month - 6 < month < date_today.month) | (
                    (day < date_today.day) & (month == date_today.month)) | (
                          (date_today.month < 7) & (month > 7))
        if is_past:
            cron.remove(job)
            write = True
if write:
    cron.write()
