import t3_util as ut
import pandas as pd

detalhes_items = ut.conecta_mongo('teste')
df = pd.DataFrame(list(detalhes_items))

print(df)