# Inspired by Prefect's cron scheduler
# https://github.com/PrefectHQ/prefect/blob/16f46ca49838cbdb250d113bb149f13ca819302d/src/prefect/client/schemas/schedules.py#L92

from ray.workflow.event_listener import EventListener
from ray.workflow.common import Event
from typing import Optional
from croniter import croniter
from pydantic import Field, validator
from pydantic import BaseModel
import datetime
import asyncio
from datetime import datetime
import time
import dateutil.tz
# import pytz

       


class CronScheduleListener(EventListener):
    """
    A listener that produces an event for a given cron schedule. To be used as a high-level lightweight
    scheduling utility for Ray Workflows without having to setup an external scheduler like Airflow.

    NOTE: Cron's rules for DST are based on schedule times, not intervals. 
    This means that an hourly cron schedule will fire on every new schedule hour,
    not every elapsed hour; for example, when clocks are ret back this will result
    in a two-hour pause. The schedule will fire at 1am and then again at 2am,
    which in this case will be 120 minutes later.  For longer schedules, such as a daily
    one, it will automatically adjust for DST.

    Args:
        cron (str): a valid cron string
        timezone (str): a valid timezone string in IANA tzdata format (i.e. America/New_York)
        day_or (bool, optional): Control how croniter handles 'day' and 'day_of_week' entries.
          Defaults to True, matching cron which connects those values using OR. If the switch
          is set to False, the values are connected using AND. This behaves like fcron so you
          can do stuff like define a job that executes every 3rd wednesday of a month by setting
          the days of month and the weekday. 
    
    """

    def __init__(
            self, 
            cron: str = Field(default=..., example="0 0 * * *"),
            timezone: Optional[str] = Field(default=None, example="America/New_York"),
            # day_or: bool = Field(
            #     default=True,
            #     description=(
            #         "Control croniter behaviour for handling day and day of week entries."
            #     ),
            # )
        ):

        self.cron = cron
        self.timezone = timezone
        # self.day_or = day_or

            # max_iter: Optional[int] = Field(default=1000, example=9001)


    # @validator("cron")
    # def validate_cron_string(cls, v):
    #     # Not supporting "random" and "hashed" croniter expressions
    #     if not croniter.is_valid(v):
    #         raise ValueError(f'Invalid cron string: "{v}"')
    #     elif any(c for c in v.split() if c.casefold() in ["R", "H", "r", "h"]):
    #         raise ValueError(
    #             f'Random and Hashed expressions are unsupported: "{v}"'
    #         )
    #     return v

    # @validator("timezone")
    # def valid_timezone(cls, v):
    #     if v and v not in pendulum.tz.timezones:
    #         raise ValueError(
    #             f'Invalid timezone: "{v} (specify in IANA tzdata format, for example,)'
    #             " America/New_York"
    #         )
    #     return v


    def validate_cron_string(cls, cron_string):
        if not croniter.is_valid(cron_string):
            raise ValueError(f'Invalid cron string: "{cron_string}"')
        elfi 

    async def poll_for_event(self, cron, timezone):
        # Get next from croniter
        tz = dateutil.tz.gettz(timezone)
        current_time = dateutil.parser.parse("now", tz=tz)
        # current_time = time.time()s

        next_date = croniter(cron, current_time).get_next()
        await asyncio.sleep(next_date - current_time)

