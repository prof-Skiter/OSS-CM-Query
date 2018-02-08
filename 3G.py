# -*- coding: utf-8 -*-
"""
Created on Wed Feb  7 15:29:01 2018

@author: skiter
"""
from sqlalchemy import create_engine
import pandas as pd
import datetime, re, os

engine = create_engine('oracle://aurfeng:Default1@T4OSS')
connection = engine.connect()

results = connection.execute("""
                             SELECT OBJ.CO_DN,
                                    OBJ.CO_NAME,
                                    WCEL.WCEL_C_ID,
                                    WCEL.WCEL_UARFCN,
                                    WCEL.WCEL_PRI_SCR_CODE,
                                    WCEL.WCEL_ACS_31,
                                    WCEL.WCEL_CELL_BARRED,
                                    WCEL.WCEL_CELL_RESERVED,
                                    WBTS.WBTS_BTSIP_ADDRESS
                             FROM C_RNC_WCEL WCEL
                             LEFT JOIN CTP_COMMON_OBJECTS OBJ ON WCEL.OBJ_GID = OBJ.CO_GID
                             LEFT JOIN C_RNC_WBTS  WBTS ON OBJ.CO_PARENT_GID = WBTS.OBJ_GID
                             WHERE WCEL.CONF_ID = 1 AND WBTS.CONF_ID = 1
                             """).fetchall()
df = pd.DataFrame(results)
df.columns = results[0].keys()
df.co_dn = df.co_dn.apply(lambda x: re.search("(?<=WBTS-)(\d+)", x).group(1))
df['Site'] = df.co_name.str[1:9]
df.wcel_acs_31 = df.wcel_acs_31.map({0:'Locked',
                                     1:'Unlocked'})

df.wcel_cell_barred = df.wcel_cell_barred.map({0:'Barred',
                                               1:'Not barred'})

df.wcel_cell_reserved = df.wcel_cell_reserved.map({0:'Reserved',
                                                   1:'Not reserved'})
  
#df = df[['co_dn','Site','co_name','wcel_c_id','wcel_uarfcn','wcel_pri_scr_code','wcel_acs_31','wcel_cell_barred','wcel_cell_reserved']]


#.to_clipboard(columns=['MRBTS/WBTS','Site Name','Sector Name','Cell ID','Freq/BW','PCI/PSC','administrativeState','cellBarred','primPlmnCellres'], index=False)

print(df.groupby('Site').get_group('AU01310B'))