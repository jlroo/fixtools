
from fixtools.util import (open_fix, most_liquid)
from fixtools.book import (initial_book)
from fixtools.futures import (Futures)
from fixtools.options import (Options, options_books)

__all__ = ['most_liquid', 'open_fix', 'initial_book', 'Futures', 'Options', 'options_books']
__docformat__ = 'restructuredtext'

# Let users know if they're missing any of our hard dependencies
hard_dependencies = ("gzip", "bz2")
missing_dependencies = []

for dependency in hard_dependencies:
    try:
        __import__(dependency)
    except ImportError as e:
        missing_dependencies.append(dependency)

if missing_dependencies:
    raise ImportError("Missing required dependencies {0}".format(missing_dependencies))
del hard_dependencies, dependency, missing_dependencies
