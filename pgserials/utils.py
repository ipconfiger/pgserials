import time
import re

from .exceptions import DefinationError

r = re.compile(r'^(\d+)\w$')
UNITS = {'ms': 1, 's': 1000, 'm': 60 * 1000, 'h': 3600 * 1000, 'd': 3600 * 24 * 1000}


def pre_process_val(v):
    if isinstance(v, int) or isinstance(v, float):
        return v
    if isinstance(v, str):
        return v.strip()
    return v


def csv_val(v):
    if isinstance(v, int) or isinstance(v, float):
        return str(v)
    if isinstance(v, str):
        return f"'{v.strip()}'"
    return v


def generate_timestamp(s):
    add = True
    ts = int(time.time() * 1000)
    if s == '':
        return ts
    if s.startswith('-'):
        add = False
        s = s[1:]
    if not r.match(s):
        raise DefinationError('Invalid time string')
    n = int("".join([w for w in s if w.isdigit()]))
    unit = "".join([w for w in s if not w.isdigit()])
    key = unit.lower() if unit.lower() == 'ms' else unit[:1].lower()
    if key not in ['ms', 's', 'm', 'h', 'd']:
        raise DefinationError('Invalid time string:unit must be in ms/s/m/h/d')
    milliseconds = n * UNITS.get(key)
    return ts + milliseconds if add else ts - milliseconds
