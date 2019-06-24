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


from flask import Flask, request

import os 
from io import BytesIO, StringIO
from scipy import sparse
from google.cloud import storage


app = Flask(__name__)


BROWSE_SCORE = 0.5
BASKET_SCORE = 2.5
TFT_FOLDER = 'gs://papis19wjf/tf_transform'
MODEL_FOLDER = 'gs://papis19wjf/trainer/sim_matrix'
BUCKET_NAME = 'papis19wjf'


def load_data():
    file_ = BytesIO()
    #download_blob(BUCKET_NAME, 'trainer/sim_matrix', file_)
    file_.seek(0)

    #sim_matrix = sparse.load_npz(file_)
    sim_matrix = None

    file_ = BytesIO()
    download_blob(BUCKET_NAME, 'tft_transform/transform_fn/assets/skus_mapping', file_)
    skus_mapping = file_.readlines()
    print(skus_mapping[:10])
    skus_mapping = dict(enumerate(skus_mapping))

    return sim_matrix, skus_mapping


def download_blob(bucket_name, source_blob_name, destination_file_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
 
    blob.download_to_file(destination_file_name)


sim_matrix, skus_mapping = load_data()

@app.route('/make_recommendations', methods=['GET'])
def make_recommendations():
    return str(skus_mapping)


def predict_json(project, model, instances, version=None):
    """Send json data to a deployed model for prediction.

    Args:
        project (str): project where the AI Platform Model is deployed.
        model (str): model name.
        instances ([Mapping[str: Any]]): Keys should be the names of Tensors
            your deployed model expects as inputs. Values should be datatypes
            convertible to Tensors, or (potentially nested) lists of datatypes
            convertible to tensors.
        version: str, version of the model to target.
    Returns:
        Mapping[str: any]: dictionary of prediction results defined by the
            model.
    """
    # Create the AI Platform service object.
    # To authenticate set the environment variable
    # GOOGLE_APPLICATION_CREDENTIALS=<path_to_service_account_file>
    service = googleapiclient.discovery.build('ml', 'v1')
    name = 'projects/{}/models/{}'.format(project, model)

    if version is not None:
        name += '/versions/{}'.format(version)

    response = service.projects().predict(
        name=name,
        body={'instances': instances}
    ).execute()

    if 'error' in response:
        raise RuntimeError(response['error'])

    return response['predictions']


if __name__ == "__main__":
    app.run(host='0.0.0.0')
