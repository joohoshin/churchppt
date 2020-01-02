# -*- coding: utf-8 -*-

import sqlalchemy as sa
import glob
import os
import pandas as pd

con_str = 'mysql+pymysql://root:machmach2!@127.0.0.1/bible'
path = 'C:\\Users\\Jh\\OneDrive - Shinsung Delta Tech Co., Ltd\\church\\Text-개역개정'
files = glob.glob(path+'\\*.txt')

bible_db = sa.create_engine(con_str).connect()

isfirst = True
for f in files:    
    #f= files[0]
    print(os.path.basename(f))     
    filename = os.path.basename(f)
    res_df = pd.read_csv(f, engine = 'python', sep = '\t', header=None)
    res_df = res_df[0].str.split(' ',n=1, expand=True)
    res_df = res_df.rename(columns = {0:'chapter', 1:'text'})    
    res_df['filename'] = filename
    
    if isfirst :
        res_df.to_sql('bible', bible_db, if_exists='replace', index=False)    
        isfirst = False
    else:
        res_df.to_sql('bible', bible_db, if_exists='append', index=False)    
    
bible_db.close()