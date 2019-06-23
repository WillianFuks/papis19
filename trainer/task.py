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


import argparse
import tensorflow as tf

import model


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--tft_working_dir',
        help=('points to where the results of tftransform were exported to in the ',
              'preprocess step'),
        type=str,
        default=''
    )

    parser.add_argument(
        '--browse_score',
        help='how many points should be associated to each browsing action',
        type=float
    )

    parser.add_argument(
        '--basket_score',
        help='how many points should be associated to each add to basket  action',
        type=float
    )

    parser.add_argument(
        '--input_train_data_path',
        help='where training data is located. Can be also a GCS bucket.',
        type=str
    )

    args = parser.parse_args()
    return args


def main(args):
    estimator = model.build_estimator()

    input_fn = model.make_train_input_fn(
        args.input_train_data_path,
        args.browse_score,
        args.basket_score
    )

    estimator.train(
        input_fn=input_fn
    )        
    

if __name__ == '__main__':
    args = parse_args()
    main(args)
