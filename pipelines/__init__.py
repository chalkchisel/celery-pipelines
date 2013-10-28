from pipelines.models import (
    PIPELINES, SynchronousPipeline, ParallelPipeline,
    register_task as task,
    register_subpipeline as subpipeline)

__all__ = ['PIPELINES', 'SynchronousPipeline',
           'ParallelPipeline', 'task', 'subpipeline']
