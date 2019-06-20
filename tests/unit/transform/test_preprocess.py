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


from transform.preprocess import build_bq_query


def test_build_bq_query():
    query = build_bq_query('retrieve_data.sql', 'test_project', '1', '2')
    assert query == '1'



