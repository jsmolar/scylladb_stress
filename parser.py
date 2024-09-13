import dataclasses
import re
from typing import Optional


@dataclasses.dataclass
class StressResult:
    """Data class to store stress test results"""

    op_rate: Optional[float] = None
    latency_mean: Optional[float] = None
    latency_99th_percentile: Optional[float] = None
    latency_max: Optional[float] = None


def _sanitize(data):
    """Removes unused part of output, leaves only Results"""
    return re.sub(r".*Results:", "Results:", data, flags=re.DOTALL)


def parse(data):
    """
    Parses output from stress test. Fields defined in `StressResult` are used as keys in regex
    (underscores are swapped for spaces, to reflect real keys).
    """
    data = _sanitize(data)
    result = StressResult()

    for field in dataclasses.fields(StressResult):
        field_name = field.name.replace("_", " ")
        value = re.search(
            rf"\n{field_name}\s*:\s*([\d,.]+)", data, re.IGNORECASE
        ).group(1)
        value = float(value.replace(",", ""))
        setattr(result, field.name, value)

    return result
