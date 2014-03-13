from celery_pipelines.models import (
    PIPELINES, SynchronousPipeline, ParallelPipeline, ChordPipeline,
    register_task as task,
    register_subpipeline as subpipeline,
    register_callback as callback)

__all__ = ['PIPELINES', 'SynchronousPipeline',
           'ParallelPipeline', 'ChordPipeline', 'task', 'subpipeline', 'callback']


def autodiscover():
    """
    Auto-discover INSTALLED_APPS pipelines.py modules and fail silently when
    not present. This forces an import on them to register any pipeline bits they
    may want.
    """

    import copy
    from django.conf import settings
    from django.utils.importlib import import_module
    from django.utils.module_loading import module_has_submodule

    for app in settings.INSTALLED_APPS:
        mod = import_module(app)
        # Attempt to import the app's admin module.
        try:
            before_import_registry = copy.copy(PIPELINES._pipeline_registry)
            import_module('%s.pipelines' % app)
        except:
            # Reset the model registry to the state before the last import as
            # this import will have to reoccur on the next request and this
            # could raise NotRegistered and AlreadyRegistered exceptions
            # (see #8245).
            PIPELINES._pipeline_registry = before_import_registry

            # Decide whether to bubble up this error. If the app just
            # doesn't have an admin module, we can ignore the error
            # attempting to import it, otherwise we want it to bubble up.
            try:
                if module_has_submodule(mod, 'pipelines'):
                    raise
            except DeprecationWarning:
                pass
