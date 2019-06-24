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


import tensorflow as tf
import numpy as np
from scipy import sparse
from io import BytesIO

import trainer.metadata as metadata

from google.cloud import storage


def train_and_save(filenames, browse_score, basket_score, output_filename):
    files = tf.gfile.Glob(filenames)

    # build a lookup table
    table = tf.contrib.lookup.HashTable(
        tf.contrib.lookup.KeyValueTensorInitializer(
            keys=tf.constant(['Browsed', 'AddedToBasket']),
            values=tf.constant([browse_score, basket_score], dtype=tf.float32),
        ), -1
    )

    feature_description = metadata.INPUT_TRAIN_SCHEMA

    def _make_parser_function(feature_description):
        def _parse_function(example_proto):
          return tf.parse_single_example(example_proto, feature_description)
        return _parse_function
    
    _parser_fn = _make_parser_function(feature_description)

    def _process_row(row):
        """
        row: {'customer_id': int, 'actions_list': tf.Sparse, 'skus_list': tf.Sparse}
        
        returns
          same row but customer is replicated n times where n is the dimension of
          actions_list (or skus_list) and `actions_list` is mapped to its correspondents
          scores.
        """
        row['customer_id'] = tf.SparseTensor(
            indices=row['actions_list'].indices,
            values=tf.tile(tf.expand_dims(row['customer_id'], 0),
                           [tf.size(row['skus_list'])]),
            dense_shape=row['actions_list'].dense_shape
        )
        row['actions_list'] = tf.SparseTensor(
            indices=row['actions_list'].indices,
            values=table.lookup(row['actions_list'].values),
            dense_shape=row['actions_list'].dense_shape
        )
        row['skus_list'] = tf.SparseTensor(
            indices=row['skus_list'].indices,
            values=row['skus_list'].values,
            dense_shape=row['skus_list'].dense_shape
        )
        return row

    dataset = tf.data.TFRecordDataset(files) \
              .map(lambda x: _parser_fn(x)) \
              .map(lambda x: _process_row(x)) \
              .batch(1000)
    iterator = dataset.make_initializable_iterator()
    next_elem = iterator.get_next()

    data, j, i = [], [], []

    with tf.Session() as sess:
        table.init.run()
        sess.run(iterator.initializer)
        while True:
            try:
                row = sess.run(next_elem)
                data.extend(list(row['actions_list'].values))
                i.extend(list(row['customer_id'].values))
                j.extend(list(row['skus_list'].values))
            except tf.errors.OutOfRangeError:
                break
        
    users_skus_interactions = sparse.coo_matrix((data, (i, j)), shape=(np.max(i) + 1,
                                                np.max(j) + 1)).tocsc()


    # ... code to build similarity sparse matrix ...


    save_sparse_matrix(sim_matrix, output_filename)


def save_sparse_matrix(matrix, path):
    if 'gs://' in path:
        file_ = BytesIO()
        sparse.save_npz(file_, matrix)
        file_.seek(0)

        storage_client = storage.Client()

        bucket_name = path.split('/')[2]
        bucket = storage_client.get_bucket(bucket_name)
        
        destination_blob_name = '/'.join(path.split('/')[3:])
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_file(file_)
    else:
        sparse.save_npz(path, matrix)
