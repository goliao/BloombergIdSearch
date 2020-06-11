
from importlib import reload
import requests
import pandas as pd
import math
import time
import numpy as np
import utilfigi
reload(utilfigi)
import utilfigi


tomap=pd.DataFrame({'idValue':['F','TSLA'],'exchCode':'US'})
utilfigi.mapfigi(tomap,idType='TICKER')

# w/o specifying exchange code
tomap=pd.DataFrame({'idValue':['F','TSLA']})
utilfigi.mapfigi(tomap,idType='TICKER')


# search function (slower)
utilfigi.querysinglebatch(pd.DataFrame({'search':['TSLA']}))
