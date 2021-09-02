from apscheduler.schedulers import SchedulerAlreadyRunningError
from apscheduler.schedulers.background import BackgroundScheduler

from tools.logging import info

RUNNING = 1

_scheduler = BackgroundScheduler()


def start_scheduler(job, seconds):
    _scheduler.add_job(job, trigger='interval', seconds=seconds)

    try:
        _scheduler.start()
    except SchedulerAlreadyRunningError:
        info('SchedulerAlreadyRunningError')
        pass


def pause_scheduler():
    _scheduler.pause()


def resume_scheduler():
    _scheduler.resume()


def is_running():
    return _scheduler.state == RUNNING
