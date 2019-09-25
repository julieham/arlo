from apscheduler.schedulers import SchedulerAlreadyRunningError
from apscheduler.schedulers.background import BackgroundScheduler

from tools.logging import info

_scheduler = BackgroundScheduler()


def start_scheduler(job):
    _scheduler.add_job(job, trigger='interval', seconds=3600)

    try:
        _scheduler.start()
    except SchedulerAlreadyRunningError:
        info('SchedulerAlreadyRunningError')
        pass


def pause_scheduler():
    _scheduler.pause()


def resume_scheduler():
    _scheduler.resume()
