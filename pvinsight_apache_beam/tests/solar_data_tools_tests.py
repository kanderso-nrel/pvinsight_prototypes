import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from pvinsight_apache_beam.transformers.solar_data_tools_ptransforms import ExtractEstimatedCapacity

class TestSolarDataTools(unittest.TestCase):
    """
    Tests for individual transformers in the solar data tool pipeline
    """

    def test_estimated_capacity(self):
        """
        Example of how to construct a transformer unit test. Not really functional, but
        just to give you the idea of how to set up test pipelines
        """
        capacity_estimates = [
            {"site_id": "A", "capacity_estimate": 3.0},
            {"site_id": "B", "capacity_estimate": 4.2},
            {"site_id": "C", "capacity_estimate": 5.0},
            {"site_id": "D", "capacity_estimate": 6.7},
            {"site_id": "E", "capacity_estimate": 7.2},
        ]
        expected_output = [x['capacity_estimate'] for x in capacity_estimates]
        # Create a test pipeline.
        with TestPipeline() as p:
            # Make a list of capacity estimates and test a contrived transform
            # that extracts the capacity value
            output = (
                    p
                    | "Make a test Pcollection" >> beam.Create(capacity_estimates)
                    | "Extract capacity" >> beam.ParDo(ExtractEstimatedCapacity())
            )

            # Assert on the results.
            assert_that(
                output,
                equal_to(expected_output)
            )


if __name__ == '__main__':
    unittest.main()
