import logging
import rdtools
import pandas as pd
import os
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


def get_system_data(system_id):
    datadir = r'C:\Users\KANDERSO\projects\pvinsight_prototypes\datasets'
    filename = os.path.join(datadir, f'{system_id}.csv')
    df = pd.read_csv(filename, index_col=0, parse_dates=True)
    df = df.tz_localize('Etc/GMT+7')
    return df


def get_system_metadata(system_id):
    return {
        'gamma_pdc': -0.005,
        'interp_freq': '1T',
        'temperature_model': 'open_rack_glass_polymer',
    }


class GetDataDoFn(beam.DoFn):
    def process(self, system_id):
        df = get_system_data(system_id)
        metadata = get_system_metadata(system_id)
        # use yield instead of return to keep it as a unit instead of having
        # it be unpacked as an iterable.  I don't fully understand this...
        yield (system_id, df, metadata)


class RdToolsDegradationDoFn(beam.DoFn):
    def process(self, arg):
        system_id, df, metadata = arg
        ta = rdtools.TrendAnalysis(df['ac_power'],
                                   df['poa_irradiance'],
                                   temperature_ambient=df['ambient_temp'],
                                   windspeed=df['wind_speed'],
                                   **metadata)
        ta.sensor_analysis(analyses=['yoy_degradation'])
        yoy_results = ta.results['sensor']['yoy_degradation']
        rd = yoy_results['p50_rd']
        ci = yoy_results['rd_confidence_interval']
        yield [system_id, rd, ci[0], ci[1]]


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        system_ids = p | beam.Create([
            'pvdaq4',
            'pvdaq5',
        ])
        _ = (
            system_ids
            | beam.ParDo(GetDataDoFn())
            | beam.ParDo(RdToolsDegradationDoFn())
            | beam.Map(print)
        )

if __name__ == '__main__':
    # logging.getLogger().setLevel(logging.INFO)
    run()
    
