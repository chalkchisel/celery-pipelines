from celery import Task, group, chain, chord
from celery.app.task import TaskType
from celery import current_app
from celery.contrib.methods import task_method

from collections import OrderedDict


class PipelineDoesNotExist(Exception):
    pass


class PipelineHasNoTasks(Exception):
    pass


class PipelineBroker:

    """
    Class to handle pipeline registration and master task ordering
    """

    def __init__(self):
        self._task_ordering = 0
        self._pipeline_registry = OrderedDict()

    def start(self, pipeline_name, *args, **kwargs):
        """
        Starts a pipeline by name
        """
        pipeline = self._pipeline_registry.get(pipeline_name, None)
        if not pipeline:
            raise PipelineDoesNotExist('Pipeline "%s" has not been registered.' % pipeline_name)

        return pipeline().start(*args, **kwargs)

    def _register_pipeline(self, pipeline_name, pipeline):
        """
        Register a pipeline with the pipeline registry.

        Durring the process of registration, all ordered members
        (hasattr(x, "order") == True) will be converted to method
        tasks.
        """

        pipeline_tasks = []

        for name, method in pipeline.__dict__.iteritems():
            if hasattr(method, "order"):
                task = current_app.task(
                    method, filter=task_method, name="%s.%s" % (pipeline.name, name))
                setattr(pipeline, name, task)
                pipeline_tasks.append((method.order, name))

        sorted_pipeline_tasks = [p[1]
                                 for p in sorted(pipeline_tasks, key=lambda p: p[0])]

        pipeline.identifier = pipeline_name
        pipeline.tasks = sorted_pipeline_tasks
        self._pipeline_registry[pipeline_name] = pipeline
        return pipeline


PIPELINES = PipelineBroker()


class PipelineTask(Task):

    """
    Base individual pipeline task object
    """
    abstract = True
    identifier = None
    ignore_result = False
    pipeline = None


class PipelineBase(TaskType):

    """
    Pipeline metaclass to allow for registration with the PipelineBroker on class creation
    """
    def __new__(cls, name, bases, attrs):
        new = super(PipelineBase, cls).__new__
        pipeline_klass = new(cls, name, bases, attrs)

        mod = pipeline_klass.__module__
        if mod == "celery_pipelines.models":
            return pipeline_klass

        app_name, sep, after = mod.partition('.pipelines')
        identifier = app_name + "." + pipeline_klass.__name__
        if identifier and identifier not in PIPELINES._pipeline_registry:
            return PIPELINES._register_pipeline(identifier, pipeline_klass)
        return pipeline_klass


class BasePipeline(PipelineTask):

    """
    Base Pipeline Class
    """
    abstract = True
    tasks = None
    signature = None

    __metaclass__ = PipelineBase

    def __init__(self, *args, **kwargs):
        self.wait_for_result = True
        super(BasePipeline, self).__init__(*args, **kwargs)

    def build_signature(self, *args, **kwargs):
        if not self.tasks:
            raise PipelineHasNoTasks('Pipeline "%s" has no tasks.')

        # we need to manually pass self here in kwargs and then handle making it work right in the decorator...
        # currently celery doesn't handle this use case
        kwargs.update({
            '__self__': self
        })

        subtasks = []
        for i, task in enumerate(self.tasks):
            t = getattr(self, task)
            t = t.s(*args, **kwargs)
            subtasks.append(t)

        return self.signature(*subtasks)

    def run(self, *args, **kwargs):
        """
        Run the pipeline
        """
        signature = self.build_signature(*args, **kwargs)

        # using the proper signature, spawn a task and do the work
        result = signature.apply_async()

        if self.wait_for_result:
            return result.get()  # will return a single result
        return result

    def start(self, *args, **kwargs):
        """
        Start the pipeline and either return the actual result or the result class, based on a a parameter
        """
        self.wait_for_result = kwargs.pop('wait_for_result', True)
        return self.run(*args, **kwargs)


class SynchronousPipeline(BasePipeline):

    """
    A pipeline that executes synchronously
    """
    abstract = True
    signature = chain


class ParallelPipeline(BasePipeline):

    """
    A Pipeline that executes in parallel
    """
    abstract = True
    signature = group


class ChordPipeline(BasePipeline):

    """
    A pipeline that executes in parallel and then executes a callback when all tasks are finished
    """
    abstract = True
    signature = chord

    def callback(self, *args, **kwargs):
        raise NotImplementedError('Callback must be implemented for a ChordPipeline.')

    def run(self, *args, **kwargs):
        """
        Run the pipeline
        """
        signature = self.build_signature(*args, **kwargs)

        # build the callback task
        decorated = register_task(self.callback)
        callback = current_app.task(
            decorated, filter=task_method, name="%s.chord_callback" % (self.name))

        result = signature(callback.s())  # start the chord

        return result.get()  # will return a single result


def register_task(method):  # AKA: bolster.pipelines.task
    """
    Decorator that allows for ordered registration of tasks to a pipeline.

    In addition, the decorator takes care of properly passing "self" to the
    function (which it gets from kwargs)

    The return value of this decorator is still a 'normal' method. Actual
    creation of the task is done by registering the pipeline with the
    PipelineBroker.
    """
    def decorator(*args, **kwargs):
        _self = kwargs.pop('__self__', args[0] if len(args) > 0 else None)  # handle self
        if _self in args:
            arg_list = list(args)
            arg_list.remove(_self)
            args = tuple(arg_list)
        return method(_self, *args, **kwargs)

    decorator.order = PIPELINES._task_ordering
    decorator.task_type = "task"
    PIPELINES._task_ordering += 1
    return decorator


def register_subpipeline(method):  # AKA: bolster.pipelines.subpipeline
    """
    Registers a subpipeline by creating a task to spawn the pipeline.

    The method that this decorator wraps must return an uninstantiated
    subclass of PipelineBase.
    """

    def subpipeline_task(self, *args, **kwargs):
        # instantiate the pipeline that was returned, being sure to wait for
        # the result because its a sub pipeline
        pipeline = method(self, *args, **kwargs)()
        # shoot off the task and wait
        return pipeline.start(*args, wait_for_result=True, **kwargs)

    # call register_task on the decorator so that this method get's picked up
    # properly when the pipeline is registered
    decorator = register_task(subpipeline_task)
    decorator.task_type = "pipeline"

    return decorator
