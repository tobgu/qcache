from __future__ import unicode_literals

import operator

JOINING_OPERATORS = {'&': operator.and_,
                     '|': operator.or_}

COMPARISON_OPERATORS = {'==': operator.eq,
                        '!=': operator.ne,
                        '<': operator.lt,
                        '<=': operator.le,
                        '>': operator.gt,
                        '>=': operator.ge}

FILTER_ENGINE_PANDAS = 'pandas'
FILTER_ENGINE_NUMEXPR = 'numexpr'
