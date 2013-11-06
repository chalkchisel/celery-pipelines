from pipelines import *


# SET UP Test Pipelines

class SynchronousPipelineTest(SynchronousPipeline):

    """
    SYNCHRONOUS PIPELINE
    """
    @task
    def task1(self):
        return 2

    @task
    def task2(self, prev_result):
        return prev_result + 4


class ParallelPipelineTest(ParallelPipeline):

    """
    PARALLEL PIPELINE
    """
    identifier = 'test_parallel_pipeline'

    @task
    def task1(self):
        return 2

    @task
    def task2(self):
        return 4


class ComposedPipeline(SynchronousPipeline):

    """
    SYNCHRONOUS COMPOSED w/ PARALLEL PIPELINE
    """
    name = 'synchronous_composed_w_parallel_pipeline'
    task2_data = 4

    @task
    def task1(self):
        return 2

    @subpipeline
    def task_inner(self, *args, **kwargs):
        return InnerComposedPipeline

    @task
    def task2(self, prev_result):
        s = sum(prev_result)  # prev_result is an array
        return s + self.task2_data


class InnerComposedPipeline(ParallelPipeline):

    """
    INNER COMPOSED PIPELINE
    """

    @task
    def task1(self, prev_result, instance=None):
        if instance:
            instance['status']['inner_composed_1'] = True
        return prev_result + 8

    @task
    def task2(self, prev_result, instance=None):
        if instance:
            instance['status']['inner_composed_2'] = True
        return prev_result + 3


class ComposedPipelineWithArgs(SynchronousPipeline):

    """
    Composed Pipeline that accepts keyword arguments and modifies the passed in object
    through the steps
    """

    @task
    def task1(self, instance=None, **kwargs):
        if instance:
            instance['status']['composed_1'] = True
        return 4

    @subpipeline
    def task_inner(self, *args, **kwargs):
        return InnerComposedPipeline

    @task
    def task2(self, prev_result, instance=None, *args, **kwargs):
        result = sum(prev_result) + 3
        if instance:
            instance['status']['composed_2'] = True
            instance['result'] = result
        return result
