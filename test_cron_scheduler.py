from cron_scheduler import CronScheduleListener

import ray
from ray import workflow 
import time

ray.init(storage='/home/luquoo/projects/ray_workflow_enhancement/workflow_data')

cron = '* * * * * 15,30,45' 

cron_task = workflow.wait_for_event(
     CronScheduleListener, cron
)



@ray.remote
def heads_step():
    print(f"{time.time()} heads")
    return "heads"

@ray.remote
def tails_step(*args):
    print(f'{time.time()} tails \n')

    return workflow.continuation(flip_coin.bind())


@ray.remote
def gather(*args):
    return args


@ray.remote
def flip_coin():
    import random

    cron_task = workflow.wait_for_event(CronScheduleListener, '* * * * * 5,10,15,20,25,30,35,40,45,50,55')

    @ray.remote
    def decide(heads: bool, cron_task):
        if heads:
            return workflow.continuation(heads_step.bind())

        else:
            return workflow.continuation(tails_step.bind(cron_task))

    dag = decide.bind(random.random() > 0.9, cron_task)

    return workflow.continuation(dag)


workflow.run(flip_coin.bind())


