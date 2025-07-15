#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.

import sys
import time
import traceback
from collections import deque
from typing import Deque

# Module Specifc global variable to store flamegraph values
# Has to be global since new _flamegraph_tracer() gets called each time
profile_last_time = time.time()
global_flamegraph_deque: Deque[str] = deque()


def end_tracer():
    global global_flamegraph_deque
    sys.setprofile(None)
    flamegraph_value = "".join(global_flamegraph_deque)
    global_flamegraph_deque = deque()
    return flamegraph_value


def _mice_tracer(frame, event, arg=None):
    global global_flamegraph_deque
    global profile_last_time
    if event == "call" or event == "return":
        time_since_last_event = str(int((time.time() - profile_last_time) * 10000))
        if time_since_last_event != "0":
            for i, stack_summary in enumerate(traceback.extract_stack(frame)):
                if i != 0:
                    global_flamegraph_deque.append(";")
                global_flamegraph_deque.append(
                    f"{stack_summary.filename} in {stack_summary.name} at {stack_summary.lineno}"
                )
            global_flamegraph_deque.append(f" {time_since_last_event}\n")
            profile_last_time = time.time()
    return


def register_flamegraph_tracer():
    global profile_last_time
    global global_flamegraph_deque
    profile_last_time = time.time()
    global_flamegraph_deque = deque()
    sys.setprofile(_mice_tracer)
