# - This file is the main entry point into your Compute Module
# - All functions defined in this file are callable using the function name as the 'queryType'
# - Each function must take 2 args, with the 2nd param being the actual "payload" of your function
# - Each function must return something serializable by `json.dumps`
#
# See the README page of this repo for more details

from dataclasses import dataclass
from compute_modules.annotations import function


@dataclass
class AddPayload:
    x: int
    y: int


@function
def add(context, event: AddPayload) -> int:
    return event.x + event.y
