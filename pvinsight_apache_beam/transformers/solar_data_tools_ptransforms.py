"""
PTransforms for solar data tools operations
"""
import apache_beam as beam
import pandas as pd
from solardatatools import DataHandler
from typing import Tuple, Iterable, TypeVar, Dict, Union

T = TypeVar('T')


@beam.typehints.with_input_types(Tuple[T, Iterable[Dict]])
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
        yield site_id, DataHandler(pd.DataFrame(ts_data))


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
        yield site_id, data_handler


@beam.typehints.with_input_types(Tuple[T, DataHandler])
@beam.typehints.with_output_types(Dict)
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
        yield {"site_id": site_id, "capacity_estimate": data_handler.capacity_estimate}


@beam.typehints.with_input_types(Dict)
@beam.typehints.with_output_types(Union[str, int, float])
class ExtractEstimatedCapacity(beam.DoFn):
    """
    Fake transform used to show a unit test
    ...

    Methods
    -------
    process(element):
        Run the process.
    """
    def process(self, element):
        yield element['capacity_estimate']
