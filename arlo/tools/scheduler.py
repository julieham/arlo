from apscheduler.schedulers import SchedulerAlreadyRunningError, SchedulerNotRunningError
from apscheduler.schedulers.background import BackgroundScheduler

from tools.logging import info, error

RUNNING = 1

_scheduler = BackgroundScheduler()


def start_scheduler(job):
    _scheduler.add_job(job, trigger='interval', seconds=3600)

    try:
        _scheduler.start()
    except SchedulerAlreadyRunningError:
        info('SchedulerAlreadyRunningError')
        pass


def pause_scheduler():
    try:
        _scheduler.pause()
    except SchedulerNotRunningError as e:
        error(e)


def resume_scheduler():
    _scheduler.resume()


def is_running():
    return _scheduler.state == RUNNING
