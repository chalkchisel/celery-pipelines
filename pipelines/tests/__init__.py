from celery.result import EagerResult

from ..utils.testing import SettingsTestCase

from .pipelines import *


class PipelineTestCase(SettingsTestCase):

    """
    Test case for pipeline system
    """

    def test_pipeline_registration(self):
        """
        Test pipeline metaclass registration
        """
        assert PIPELINES._pipeline_registry[
            'test_parallel_pipeline'].identifier == "test_parallel_pipeline"

    def test_start_pipeline(self):
        """
        Test starting a normal, synchronous pipeline and verify result
        """
        result = SynchronousPipelineTest().start()
        assert result == 6

    def test_start_pipeline_no_wait(self):
        """
        Test starting a normal, synchronous pipeline and don't wait for result
        """
        result = SynchronousPipelineTest().start(wait_for_result=False)
        assert isinstance(result, EagerResult)

    def test_start_parallel_pipeline(self):
        """
        Start a parallel pipeline by name and verify result
        """
        # also testing that we can start by name instead of by instantiating
        # the class
        result = PIPELINES.start('test_parallel_pipeline')
        assert result == [2, 4]

    def test_start_composed_pipeline(self):
        """
        Start a composed pipeline and verify result
        """
        result = ComposedPipeline().start()
        assert result == 19

    def test_passthrough_args(self):
        """
        Test passing args through the whole chain
        """
        test_obj = {
            'status': {
                'composed_1': False,
                'composed_2': False,
                'inner_composed_1': False,
                'inner_composed_2': False,
            },
            'result': -1
        }

        result = ComposedPipelineWithArgs().start(instance=test_obj)

        assert all(map(lambda x: x[1] is True, test_obj['status'].items()))
        assert result == 22
        assert test_obj['result'] == 22
