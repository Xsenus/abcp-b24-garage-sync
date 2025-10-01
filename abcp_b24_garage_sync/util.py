
from __future__ import annotations
from datetime import datetime

def slice_by_years(start: datetime, end: datetime):
    res = []
    cur = datetime(start.year, 1, 1, 0, 0, 1)
    if start > cur:
        cur = start
    while cur <= end:
        year_end = datetime(cur.year, 12, 31, 23, 59, 59)
        if year_end > end:
            year_end = end
        res.append((cur, year_end))
        cur = datetime(cur.year + 1, 1, 1, 0, 0, 1)
    return res
