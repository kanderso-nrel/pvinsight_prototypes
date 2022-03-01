"""Apache Beam Pipeline to get estimated capacity using solar_data_tools
"""
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging
from typing import Any, Dict, List
from pvinsight_apache_beam.transformers.solar_data_tools_ptransforms \
    import CreateDataHandler, SummarizePowerTimeSeries, GetEstimatedCapacity
import argparse


def run(input_query: str, output_file: str, beam_args: List[str] = None) -> None:
    """Build and run the pipeline."""
    options = PipelineOptions(beam_args)

    with beam.Pipeline(options=options) as pipeline:
        capacity_estimates = (
                pipeline
                | "Read from BigQuery" >> beam.io.Read(beam.io.BigQuerySource(query=input_query))
                # rows returned as dictionary with keys matching BQ columns
                | "Group by system id" >> beam.WithKeys(lambda row: row['site_id'])
                | "Create data handler" >> beam.ParDo(CreateDataHandler())
                | "Run pipeline" >> beam.ParDo(SummarizePowerTimeSeries())
                | "Get capacity estimate" >> beam.ParDo(GetEstimatedCapacity())
        )

        # Output the results into BigQuery table.
        _ = capacity_estimates | "Write to Big Query" >> beam.io.WriteToText(
            output_file
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_file",
        help="Output file path"
    )
    parser.add_argument(
        "--input_query",
        help="SQL query to pull power data"
    )
    args, beam_args = parser.parse_known_args()

    run(
        input_query=args.input_subscription,
        output_file=args.output_table,
        beam_args=beam_args,
    )
