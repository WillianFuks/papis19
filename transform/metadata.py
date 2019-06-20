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


RAW_DATA_SCHEMA = {
    'customer_id': dataset_schema.ColumnSchema(
        tf.string,
        [],
        dataset_schema.FixedColumnRepresentation()
    ),
    'sku': dataset_schema.ColumnSchema(
        tf.string,
        [],
        dataset_schema.FixedColumnRepresentation()
    ),
    'action': dataset_schema.ColumnSchema(
        tf.string,
        [],
        dataset_schema.FixedColumnRepresentation()
    )
}

RAW_DATA_METADATA = dataset_metadata.DatasetMetadata(
    dataset_schema.Schema(RAW_DATA_SCHEMA)
)

OUTPUT_TRAIN_SCHEMA = {
    'customer_id': dataset_schema.ColumnSchema(
        tf.string,
        [],
        dataset_schema.FixedColumnRepresentation()
    ),
    'skus_list': dataset_schema.ColumnSchema(
        tf.int64,
        [],
        dataset_schema.ListColumnRepresentation()
    ),
    'actions_list': dataset_schema.ColumnSchema(
        tf.string,
        [],
        dataset_schema.ListColumnRepresentation()
    )
}

OUTPUT_TEST_SCHEMA = {
    'customer_id': dataset_schema.ColumnSchema(
        tf.string,
        [],
        dataset_schema.FixedColumnRepresentation()
    ),
    'skus_list': dataset_schema.ColumnSchema(
        tf.int64,
        [],
        dataset_schema.ListColumnRepresentation()
    ),
    'actions_list': dataset_schema.ColumnSchema(
        tf.string,
        [],
        dataset_schema.ListColumnRepresentation()
    ),
    'trained_skus_list': dataset_schema.ColumnSchema(
        tf.int64,
        [],
        dataset_schema.ListColumnRepresentation()
    ),
    'trained_actions_list': dataset_schema.ColumnSchema(
        tf.string,
        [],
        dataset_schema.ListColumnRepresentation()
    )
}
