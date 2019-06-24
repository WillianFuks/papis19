# Copyright 2019 Willian Fuks
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import absolute_import, division, print_function

import os
import sys
import argparse

import six
import tensorflow as tf
import tensorflow_transform as tft
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import tensorflow_transform.beam.impl as tft_beam
from tensorflow_transform.tf_metadata import dataset_metadata, dataset_schema


from tensorflow_transform.beam import impl as beam_impl
from tensorflow_transform.coders import example_proto_coder
from tensorflow_transform.beam.tft_beam_io import transform_fn_io

import ast
import six

import preprocess.metadata as metadata
import tempfile


if not six.PY2:
    sys.exit("ERROR: Must use Python2.7")


def build_bq_query(filename, project_id, init_date, end_date):
    query = open(filename).read().format(project_id=project_id, init_date=init_date,
                                         end_date=end_date)
    return query


def build_pipeline_options(args):
    """
    Apache Beam Pipelines must receive a set of options for setting how the engine should
    run.

    Args
    ----
      args: argparse.Namespace

    Returns
    -------
      pipeline_options: defines how to run beam job.
    """
    options = {}
    options['runner'] = args.runner
    if args.temp_location:
        options['temp_location'] = args.temp_location
    if args.project:
        options['project'] = args.project
    if args.staging_location:
        options['staging_location'] = args.staging_location
    if args.job_name:
        options['job_name'] = args.job_name
    if args.max_num_workers:
        options['max_num_workers'] = args.max_num_workers
    if args.machine_type:
        options['machine_type'] = args.machine_type

    options.update({'save_main_session': True})
    options.update({'setup_file': './setup.py'})

    pipeline_options = PipelineOptions(**options)
    return pipeline_options


class FlattenInteractionsFn(beam.DoFn):
    def process(self, element):
        """
        flattens table
        """
        for hit in element[1]:
            yield {'customer_id': element[0], 'sku': hit['sku'], 'action': hit['action']}


def preprocess_fn(dictrow):
    return {
        'customer_id': tft.string_to_int(dictrow['customer_id'],
                                         vocab_filename='customers_mapping'),
        'sku': tft.string_to_int(dictrow['sku'], vocab_filename='skus_mapping'),
        'action': dictrow['action']
    }


def aggregate_customers_sessions(sessions):
    """
    Receives as input what products customers interacted with and returns their final
    aggregation.
    
    Args
    ----
      sessions: list of list of dicts.
          List where each element is a list of dict of type: [{'action': '', 'sku': ''}]
          
    Returns
    -------
      results: list of dicts
          Each resulting dict is aggregated on the sku and action level (repeating
          clauses are filtered out).
    """
    result = []
    for session in sessions:
        for hit in session:
            result.append(hit)
    return [dict(t) for t in {tuple(d.items()) for d in result}]


def build_final_results(row):
    """
    row = (customer_id, [{sku:, action}, {sku:, action}])
    """
    skus_list = [e['sku'] for e in row[1]]
    actions_list = [e['action'] for e in row[1]]
    return {
        'customer_id': row[0],
        'skus_list': skus_list,
        'actions_list': actions_list
    }


def build_test_results(row):
    """
    ('customer2', {'test': [{'skus_list': [1, 1], 'actions_list': ['AddedToBasket',
     'Browsed'], 'customer_id': 'customer2'}], 'train': [{'skus_list': [1, 1],
     'actions_list': ['AddedToBasket', 'Browsed'], 'customer_id': 'customer2'}]})
    """
    result = {}
    result['customer_id'] = row[0]
    
    inner_dicts = row[1]

    # customers that had empty interactions after filtering out test dataset.
    if not inner_dicts['test']: 
        return 

    # customers that were not present in training data.
    if not inner_dicts['train']: 
        return 

    test_dict = inner_dicts['test'][0]

    result['skus_list'] = test_dict['skus_list']
    result['actions_list'] = test_dict['actions_list']
    
    train_dict = inner_dicts['train'][0]
    result['trained_skus_list'] = train_dict['skus_list']
    result['trained_actions_list'] = train_dict['actions_list']
    
    return result


def read_input_data(args, pipeline, flag):
    """
    Reads train and test pipelines.

    args: input args.
    pipeline: input pipeline where all transformations will take place.
    flag: either train or test.
    """
    if args.input_sql:
        train_query = build_bq_query(args.input_sql, args.project,
                                      args.train_init_date, args.train_end_date)
        test_query = build_bq_query(args.input_sql, args.project,
                                     args.test_init_date, args.test_end_date)

        data = (
            pipeline
            | '{} read'.format(flag) >> beam.io.Read(beam.io.BigQuerySource(
                query=train_query if flag == 'train' else test_query,
                use_standard_sql=True)
            )
        )

    else:
        data = (
            pipeline
            | '{} read'.format(flag) >>  beam.io.ReadFromText(
                args.input_train_data if flag == 'train' else args.input_test_data
            )
            | '{} to json'.format(flag) >> beam.Map(lambda x: ast.literal_eval(x))
        )

    data = (
        data
        | '{} filter empty hits'.format(flag) >> beam.Filter(lambda x: x['hits'])
        | '{} prepare customer grouping'.format(flag) >> beam.Map(lambda x: (
            x['customer_id'],
            [{'action': e['action'], 'sku': e['productSku']} for e in
             x['hits'] if e['action'] in ['Browsed', 'AddedToBasket']])
        )
        | '{} group customers'.format(flag) >> beam.GroupByKey()
        | '{} aggregate customers sessions'.format(flag) >> beam.Map(lambda x: (
            x[0],
            aggregate_customers_sessions(x[1]))
        )
        | '{} flatten'.format(flag) >> beam.ParDo(FlattenInteractionsFn())
    )

    return data


def write_total_distinct_keys_to_file(data, filename, key):
    """
    Counts how many distinct items of "key" is present in data. Key here is either
    sku or customer_id.

    Args
    ----
      data: pcollection.
      filename: where to write results to.
      key: on which value to count for.
    """
    _ = (
        data
        | 'get {}'.format(key) >> beam.Map(lambda x: x[key])
        | 'group {}'.format(key) >> beam.RemoveDuplicates()
        | 'count {}'.format(key) >> beam.combiners.Count.Globally()
        | 'write {}'.format(key) >> beam.io.WriteToText(filename)
    )
    

def write_tfrecords(data, schema, filename, name):
    """
    Converts input pcollection into a file of tfrecords following schema.

    Args
    ----
      data: pcollection.
      schema: dataset_schema from tensorflow transform.
      name: str to identify operations.
    """
    _ = (
        data
        | '{} tfrecords write'.format(name) >> beam.io.tfrecordio.WriteToTFRecord(
            filename,
            coder=example_proto_coder.ExampleProtoCoder(dataset_schema.Schema(schema)))
        )


def aggregate_transformed_data(transformed_data, flag):
    """
    One of the final steps into our pipelining transformations where data that has
    been transformed (in our case, skus went from string names to integer indices) is
    aggregated on the user level.

    transformed_data: pcollection.
    flag: identifies train or test

    Returns
    -------
      transformed_data aggregated on user level.
    """
    if flag == 'test':
        transformed_data = (
            transformed_data
            | 'test filter out invalid skus' >> beam.Filter(lambda x: x['sku'] != -1)
        )
    transformed_agg_data = (
        transformed_data
        | '{} prepare grouping'.format(flag) >> beam.Map(lambda x: (
            x['customer_id'],
            {'sku': x['sku'], 'action': x['action']})
        )
        | '{} transformed agg group'.format(flag) >> beam.GroupByKey()
        | '{} final results'.format(flag) >> beam.Map(lambda x: build_final_results(x))
    )
    return transformed_agg_data


def aggregate_final_test_data(train_data, test_data):
    """
    Joins train dataset with test so that only customers that we can make recommendations
    are present in final dataset. Remember that, in order to make them, we need to know
    a priori what customers interacted with. That's why we join the train data so we
    know customers preferences when we need to interact with them with our system.
    """
    data = (
        {
            'train': train_data | 'train prepare customer key' >> beam.Map(lambda x: (
                x['customer_id'], x)),
            'test': test_data | 'test prepare customer key' >> beam.Map(lambda x: (
                x['customer_id'], x))
        }
        | 'cogroup' >> beam.CoGroupByKey()
        | 'build final rows' >> beam.Map(build_test_results)
        | 'filter customers out of test' >> beam.Filter(lambda x: x)
    )
    return data
 

def run_tft_pipeline(args):
    """
    This is where all the data we have available in our database is processed and 
    transformed into Tensorflow tfrecords for later training and testing.

    The code runs in distributed manner automatically in the engine choosen by
    the `runner` argument in input.
    """
    pipeline_options = build_pipeline_options(args)
    temp_tft_folder = (
        tempfile.mkdtemp(dir='/tmp/') if not args.tft_temp else args.tft_temp
    )
    tft_transform_folder = (
        tempfile.mkdtemp(dir='/tmp/') if not args.tft_transform else args.tft_transform
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:
        with beam_impl.Context(temp_dir=temp_tft_folder):

            train_data = read_input_data(args, pipeline, 'train')
 
            write_total_distinct_keys_to_file(train_data, args.nitems_filename,
                                              'sku')
            
            train_dataset = (train_data, metadata.RAW_DATA_METADATA)
            (train_data, transformed_train_metadata), transform_fn = (
                train_dataset | beam_impl.AnalyzeAndTransformDataset(preprocess_fn)
            )

            _ = (
                transform_fn
                | 'WriteTransformFn' >>
                    transform_fn_io.WriteTransformFn(tft_transform_folder)
            )

            train_data = aggregate_transformed_data(
                train_data,
                'train'
            )
            
            write_tfrecords(train_data, metadata.OUTPUT_TRAIN_SCHEMA,
                            args.output_train_filename,
                            'output train')

            test_data = read_input_data(args, pipeline, 'test')

            test_dataset = (test_data, metadata.RAW_DATA_METADATA)
    
            (test_data, _) = (
                (test_dataset, transform_fn) | beam_impl.TransformDataset())


            test_data = aggregate_transformed_data(
                test_data,
                'test'
            )

            test_data = aggregate_final_test_data(
                train_data,
                test_data
            )

            write_tfrecords(test_data, metadata.OUTPUT_TEST_SCHEMA,
                             args.output_test_filename, 'output test')


def main():
    args = parse_args()
    run_tft_pipeline(args)


if __name__ == '__main__':
    main()
