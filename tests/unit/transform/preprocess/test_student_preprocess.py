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


"""
Unittests performed for the file preprocess.py in folder transform.
"""


from __future__ import absolute_import, division, print_function

import tempfile
import os
import ast

import pytest
import tensorflow as tf
from transform.preprocess.preprocess import (build_bq_query, run_tft_pipeline,
                                             build_pipeline_options, read_input_data)
from argparse import Namespace
import apache_beam as beam
from apache_beam.testing.util import open_shards


@pytest.fixture
def input_args():
    temp_location = tempfile.mkdtemp(dir='/tmp/')
    transform_location = tempfile.mkdtemp(dir='/tmp/')

    args = Namespace(
        input_train_data='tests/fixtures/transform/input_train_data_mock.json',
        input_test_data='tests/fixtures/transform/input_test_data_mock.json',
        runner='DirectRunner',
        temp_location=temp_location,
        project='',
        staging_location=temp_location,
        job_name='',
        max_num_workers=1,
        tft_temp=temp_location,
        input_sql='',
        nitems_filename='/tmp/papis/nitems',
        tft_transform=transform_location,
        output_train_filename=temp_location + '/output_train',
        output_test_filename=temp_location + '/output_test',
        requirements_file='',
        machine_type=''
    )
    return args


def test_build_bq_query():
    query = build_bq_query('transform/preprocess/retrieve_data.sql', 'test_project', '1', '2')

    expected = """
SELECT 
  date,
  customer_id,
  --hits
  ARRAY(SELECT AS STRUCT action AS action, productSku AS productSku FROM UNNEST(hits) WHERE action IN ('Browsed', 'AddedToBasket')) AS hits -- cheatting!
FROM `test_project.papis19.dafiti_data`
WHERE TRUE
  AND ARRAY_LENGTH(hits) > 1
  AND EXISTS(SELECT 1 FROM UNNEST(hits) WHERE action = 'Browsed')
  AND date BETWEEN '1' AND '2'
"""[1:]

    assert query == expected


def test_run_tft_pipeline(input_args):
    run_tft_pipeline(input_args)

    with open_shards(input_args.nitems_filename + '-*-of-*') as result_file:
        data = result_file.read().strip()
        assert data == '3'

    skus_mapping = os.path.join(input_args.tft_transform, 'transform_fn', 'assets',
                                'skus_mapping')
    with open(skus_mapping) as f:
        skus = f.read().split('\n')
        assert 'sku0' in skus
        assert 'sku1' in skus
        assert 'sku2' in skus

    inv_skus_mapping = dict(enumerate(skus))
    skus_mapping = {v: int(k) for k, v in inv_skus_mapping.iteritems()}

    customers_mapping = os.path.join(input_args.tft_transform, 'transform_fn', 'assets',
                                     'customers_mapping') 
    with open(customers_mapping) as f:
        customers = f.read().split('\n')
        assert 'customer0' in customers
        assert 'customer2' in customers
        assert 'customer4' in customers

    inv_customers_mapping = dict(enumerate(customers))
    customers_mapping = {v: int(k) for k, v in inv_customers_mapping.iteritems()}

    examples = []
    for record in tf.python_io.tf_record_iterator(
            path=input_args.output_train_filename + '-00000-of-00001'
        ):
        example = tf.train.Example()
        example.ParseFromString(record)
        examples.append(example)

    train_expected = {
        'customer0': {
            'skus_list': ['sku0', 'sku1', 'sku2'],
            'actions_list': ['Browsed', 'Browsed', 'Browsed']
        },
        'customer2': {
            'skus_list': ['sku0', 'sku0', 'sku2'],
            'actions_list': ['Browsed', 'AddedToBasket', 'Browsed']
        },
        'customer4': {
            'skus_list': ['sku1', 'sku2'],
            'actions_list': ['Browsed', 'Browsed']
        }
    }

    for example in examples:
        feature = dict(example.features.feature) # We are processing protobuf messages

        customer_id = feature['customer_id'].int64_list.value[0]
        customer = inv_customers_mapping[customer_id]

        skus_list = map(lambda x: inv_skus_mapping[x],
                        feature['skus_list'].int64_list.value)
        expected_skus_list = train_expected[customer]['skus_list']
        assert sorted(skus_list) == sorted(expected_skus_list)
 
        actions_list = feature['actions_list'].bytes_list.value
        expected_actions_list = train_expected[customer]['actions_list']
        assert sorted(actions_list) == sorted(expected_actions_list)

    # test dataset
    examples = []
    for record in tf.python_io.tf_record_iterator(
            path=input_args.output_test_filename + '-00000-of-00001'
        ):
        example = tf.train.Example()
        example.ParseFromString(record)
        examples.append(example)

    test_expected = {
        'customer0': {
            'skus_list': ['sku1'],
            'actions_list': ['Browsed']
        },
        'customer2': {
            'skus_list': ['sku0', 'sku0'],
            'actions_list': ['Browsed', 'AddedToBasket']
        }
    }

    for example in examples:
        feature = dict(example.features.feature) # We are processing protobuf messages

        customer_id = feature['customer_id'].int64_list.value[0]
        customer = inv_customers_mapping[customer_id]

        skus_list = map(lambda x: inv_skus_mapping[x],
                        feature['skus_list'].int64_list.value)
        trained_skus_list = map(lambda x: inv_skus_mapping[x],
                                feature['trained_skus_list'].int64_list.value)

        expected_skus_list = test_expected[customer]['skus_list']
        trained_expected_skus_list = train_expected[customer]['skus_list']

        assert sorted(skus_list) == sorted(expected_skus_list)
        assert sorted(trained_skus_list) == sorted(trained_expected_skus_list)
        
        actions_list = feature['actions_list'].bytes_list.value
        trained_actions_list = feature['trained_actions_list'].bytes_list.value
        expected_actions_list = test_expected[customer]['actions_list']
        trained_expected_actions_list = train_expected[customer]['actions_list']
        assert sorted(actions_list) == sorted(expected_actions_list)
        assert sorted(trained_actions_list) == sorted(trained_expected_actions_list)


def test_read_input_data(input_args):
    # Observe from the previous test how we implemented a strategy for testing
    # the whole pipelining process.

    # Here the task will be a bit simpler, only the read_input_data should be tested.
    # The idea is, create a pipeline, use it in the function and create a transformation
    # that writes this data somewhere. Then, read the data and see if results are as
    # expected.
    assert False
