## Celery-Beat-Redis

An implementation of celery beat, with a redis backend.  
With this module, you will be able to dynamically add task entry to task scheduler.  

### Major concepts
Before you go, you should understand these concepts:  

**Task** is normally a function you decorated with celery app.  
```python
from celery import Celery

celery_app = Celery()

@celery_app.task(name="printer")
def python_print(msg):
    print("msg: %s" % msg)
```

**Scheduler** collects entries at a specific frequency.  
**Entry** is a record of task execution plan.  

When we dynamically add a task, we are actually storing necessary info into redis.  
Scheduler can restore an task entry from that info and send it to celery worker service to make it run.
