#
# Copyright 2018 The Wallaroo Authors.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
#  implied. See the License for the specific language governing
#  permissions and limitations under the License.
#

from integration.logger import set_name

set_name(name="autoscale")

from resilience import (Crash,
                        Grow,
                        Recover,
                        Shrink,
                        Wait,
                        _test_resilience)
from test_creator import Creator

import sys
this = sys.modules[__name__]  # Passed to create_test

TC = Creator(this)

CMD_PONY = 'multi_partition_detector --depth 1'
CMD_PYTHON = 'machida --application-module multi_partition_detector --depth 1'
CMD_PYTHON3 = 'machida3 --application-module multi_partition_detector --depth 1'

VALIDATION_CMD = 'python ../../apps/multi_partition_detector/_validate.py --output {out_file}'

APIS = {'pony': CMD_PONY, 'python': CMD_PYTHON, 'python3': CMD_PYTHON3}

# If resilience is on, add --run-with-resilience to commands
import os
if os.environ.get("resilience") == 'on':
    for a in APIS:
        APIS[a] += ' --run-with-resilience'

##############
# Test spec(s)
##############

AUTOSCALE_TEST_NAME_FMT = 'test_autoscale_{api}_{source_type}_{source_number}_{ops}'

#################
# Autoscale tests
#################

OPS = [Grow(1), Grow(4), Shrink(1), Shrink(4)]
SOURCE_TYPES = ['tcp', 'gensource', 'alo']
SOURCE_NAME = 'Detector'
SOURCE_NUMBERS = [1, 2]

# Programmatically create the tests, do the name mangling, and place them
# in the global scope for pytest to find
for api, cmd in APIS.items():
    for o1 in OPS:
        for o2 in OPS:
            if o1 == o2:
                op_seq = [o1]
            else:
                op_seq = [o1, Wait(2), o2]
            for src_type in SOURCE_TYPES:
                if src_type != "gensource":
                    for src_num in SOURCE_NUMBERS:
                        TC.create(test_name_fmt = AUTOSCALE_TEST_NAME_FMT,
                                  api = api,
                                  cmd = cmd,
                                  ops = op_seq,
                                  validation_cmd = VALIDATION_CMD,
                                  source_name = SOURCE_NAME,
                                  source_type = src_type,
                                  source_number = src_num)
                else:
                    # only create 1 source for gensource
                    TC.create(test_name_fmt = AUTOSCALE_TEST_NAME_FMT,
                              api = api,
                              cmd = cmd,
                              ops = op_seq,
                              validation_cmd = VALIDATION_CMD,
                              source_name = SOURCE_NAME,
                              source_type = src_type,
                              source_number = 1)


# Sleep between tests for an arbitrary amount of time
# This is supposed to help CircleCI low-resource machines cope with all the
# processes we're starting in these tests
import time
def teardown_function(function):
    time.sleep(2)
