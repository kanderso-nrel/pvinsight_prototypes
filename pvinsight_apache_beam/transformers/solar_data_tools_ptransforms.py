"""
PTransforms for solar data tools operations
"""
import apache_beam as beam
import pandas as pd
from solardatatools import DataHandler
from typing import Tuple, Iterable, TypeVar

T = TypeVar('T')


@beam.typehints.with_output_types(Tuple[T, Iterable[dict]])
@beam.typehints.with_output_types(Tuple[T, DataHandler])
class CreateDataHandler(beam.DoFn):
    """
    PTransform which converts a PV time series to a DataHandler. Assumes that this is called
    after a GroupByKey
    ...

    Methods
    -------
    process(element):
        Process the time series data.
    """
    def process(self, element):
        (site_id, ts_data) = element
        return site_id, DataHandler(pd.DataFrame(ts_data))


@beam.typehints.with_input_types(Tuple[T, DataHandler])
@beam.typehints.with_output_types(Tuple[T, DataHandler])
class SummarizePowerTimeSeries(beam.DoFn):
    """
    Run the solar data tool pipeline on a PCollection of DataHandler objects
    ...

    Methods
    -------
    process(element):
        Run the process.
    """
    def process(self, element):
        (site_id, data_handler) = element
        data_handler.run_pipeline()
        return site_id, data_handler


@beam.typehints.with_input_types(Tuple[T, DataHandler])
@beam.typehints.with_output_types(dict)
class GetEstimatedCapacity(beam.DoFn):
    """
    Run the solar data tool pipeline on a PCollection of DataHandler objects
    ...

    Methods
    -------
    process(element):
        Run the process.
    """
    def process(self, element):
        (site_id, data_handler) = element
        return {"site_id": site_id, "capacity_estimate": data_handler.capacity_estimate}
