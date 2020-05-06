from pathlib import Path
import sys

__basepath__ = Path(sys.modules[__name__].__file__).parents[0]
__queries__ = Path(__basepath__ / 'queries')
__xwalks__ = Path(__basepath__ / 'xwalks')