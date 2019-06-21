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

import argparse
import os

from preprocess.preprocess import run_tft_pipeline



def parse_args():
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--input_train_data',
            help='training input data',
            type=str
        )

        parser.add_argument(
            '--input_test_data',
            help='training input data',
            type=str
        )

        parser.add_argument(
            '--input_sql',
            help=('query to execute against BigQuery. Only valid if `--input_train_data`'
                  ' is `None`.'),
            type=str
        )

        parser.add_argument(
            '--train_init_date',
            help='date from where to start query for training dataset. Format %Y-%m-%d',
            type=str
        )

        parser.add_argument(
            '--test_init_date',
            help='date from where to start query for test dataset. Format %Y-%m-%d',
            type=str
        )

        parser.add_argument(
            '--train_end_date',
            help='date from where to end query for training dataset. Format %Y-%m-%d',
            type=str
        )

        parser.add_argument(
            '--test_end_date',
            help='date from where to end query for test dataset. Format %Y-%m-%d',
            type=str
        )

        parser.add_argument(
            '--runner',
            help='which engine to use; defaults to "DirectRunner"',
            type=str,
            default='DirectRunner'
        )

        parser.add_argument(
            '--project',
            help='which project from GCP to run job against.',
            type=str
        )

        parser.add_argument(
            '--temp_location',
            help='temp folder to save beam jobs steps',
            type=str
        )

        parser.add_argument(
            '--max_num_workers',
            help='how many machines are allowed to spin up during job execution.',
            type=int
        )

        parser.add_argument(
            '--staging_location',
            help='staging location to savem beam jobs steps',
            type=str
        )

        parser.add_argument(
            '--job_name',
            help='identifying name for job.',
            type=str
        )

        parser.add_argument(
            '--tft_temp',
            help='folder where to save tft temp data.',
            type=str
        )

        parser.add_argument(
            '--tft_transform',
            help='folder where to save transformed metadata.',
            type=str
        )

        parser.add_argument(
            '--nitems_filename',
            help='filename for where to save total skus found in training data.',
            type=str
        )

        parser.add_argument(
            '--output_train_filename',
            help=(
                'filename for where to save final training dataset. This file feeds ',
                'the similarity matrix model creation.'
            ),
            type=str
        )

        parser.add_argument(
            '--output_test_filename',
            help=(
                'filename for where to save final test dataset. This file feeds ',
                'the similarity matrix model creation.'
            ),
            type=str
        )

        parser.add_argument(
            '--requirements_file',
            help=(
                'filename for where to save final test dataset. This file feeds ',
                'the similarity matrix model creation.'
            ),
            type=str
        )

        args = parser.parse_args()
        if not args.input_train_data and not args.input_sql:
            raise RuntimeError("Please provide either input_train_data or input_sql")

        if args.input_sql or args.runner == 'DataflowRunner':
            if not os.environ['GOOGLE_APPLICATION_CREDENTIALS']:
                raise RuntimeError('Please set env value to contain key with access to '
                                    'GCP')
        return args


def main():
    args = parse_args()
    run_tft_pipeline(args)


if __name__ == '__main__':
    main()
