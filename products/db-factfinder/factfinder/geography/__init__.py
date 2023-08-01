import math

import numpy as np


def agg_moe(x):
    return math.sqrt(sum([i ** 2 if not np.isnan(i) else 0 for i in x]))
