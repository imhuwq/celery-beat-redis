import time
import pickle
import datetime
import traceback
from uuid import uuid4

import redis
from celery import current_app
from celery.utils.log import get_logger
from celery.beat import Scheduler, event_t, heapq

from celery_beat_redis.entry import RedisScheduleEntry


class SAScheduler(Scheduler):
    UPDATE_INTERVAL = datetime.timedelta(seconds=2)

    Entry = RedisScheduleEntry

    def __init__(self, *args, **kwargs):
        self._schedule = {}
        self._last_updated = None
        Scheduler.__init__(self, *args, **kwargs)
        self.max_interval = (kwargs.get('max_interval')
                             or self.app.conf.CELERYBEAT_MAX_LOOP_INTERVAL or 5)

        redis_host = current_app.conf.REDIS_BEAT_HOST or "localhost"
        redis_port = current_app.conf.REDIS_BEAT_PORT or 6379
        redis_db = current_app.conf.REDIS_BEAT_DB or 0
        self.redis_cli = redis.StrictRedis(redis_host, redis_port, redis_db)
        self.uuid = uuid4().hex

    def tick(self, event_t=event_t, min=min,
             heappop=heapq.heappop, heappush=heapq.heappush,
             heapify=heapq.heapify, mktime=time.mktime):

        adjust = self.adjust
        interval = self.max_interval

        for entry in self.schedule.values():
            is_due, next_time_to_run = self.is_due(entry)
            if is_due:
                self.apply_entry(entry, producer=self.producer)
            interval = min(adjust(next_time_to_run), interval)
        return interval

    def setup_schedule(self):
        pass

    def requires_update(self):
        if not self._last_updated:
            return True
        return self._last_updated + self.UPDATE_INTERVAL < datetime.datetime.now()

    @property
    def objects(self):
        objs = self.redis_cli.hget(self.uuid, "objects")
        objs = pickle.loads(objs)
        return objs

    def get_from_redis(self):
        self.sync()
        records = {}
        for obj in self.objects:
            records[obj.name] = self.Entry(obj)
        return records

    @property
    def schedule(self):
        if self.requires_update():
            self._schedule = self.get_from_redis()
            self._last_updated = datetime.datetime.now()
        return self._schedule

    def sync(self):
        entries = self._schedule.values()
        for entry in entries:
            try:
                if entry.total_run_count > entry._task.total_run_count:
                    entry._task.total_run_count = entry.total_run_count
                if entry.last_run_at and entry._task.last_run_at and entry.last_run_at > entry._task.last_run_at:
                    entry._task.last_run_at = entry.last_run_at
                entry._task.run_immediately = False
            except Exception:
                get_logger(__name__).error(traceback.format_exc())
        entries = pickle.dumps(entries)
        self.redis_cli.hset(self.uuid, "objects", entries)
