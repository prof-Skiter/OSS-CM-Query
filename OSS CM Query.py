# -*- coding: utf-8 -*-
from sqlalchemy import create_engine
import pandas as pd
import datetime, re, os, sys

def query(df):
    gp = df.groupby('Site')

    while True:
        print('Querry? ', end='')
        a = input()
        if a == 'n': break
        gp\
        .get_group(a)\
        .groupby('Layer')['Site','ID','RMOD','Rfsharing','TXRX','dlMimoMode','pMax','FreqBW']\
        .agg({'Site':lambda x: str(set(x)).strip("{}'"),
              'ID':'count',
              'RMOD':lambda x: str(set(x)).strip("{}'"),
              'Rfsharing':lambda x: str(set(x)).strip("{}'"),
              'TXRX':lambda x: str(set(x)).strip("{}'"),
              'dlMimoMode':lambda x: str(set(x)).strip("{}'"),
              'pMax':lambda x: str(set(x)).strip("{}'"),
              'FreqBW':lambda x: str(set(x)).strip("{}'")})\
        .to_clipboard()

print('Pull data from OSS? ', end='')
a = input()
if a == 'n':
    curdir = os.listdir('output')
    date = datetime.datetime.strptime('2018-01-01', "%Y-%m-%d")
    for f in curdir:
        if 'CFG_QUERRY' in f:
            tmp = datetime.datetime.strptime(re.search("^\d+-\d+-\d+", f).group(0), "%Y-%m-%d")
            if tmp > date:
                date = tmp
                file = f
            
    print('Open file: ', file)
    df = pd.read_csv('output/' + file)
    print(df)
    query(df)
    sys.exit()


engine = create_engine('oracle://aurfeng:Default1@T4OSS')
connection = engine.connect()

# GET RMOD
stmt =  """
        SELECT OBJ.CO_DN, RMOD_R_PRODUCT_NAME
        FROM C_SRER_RMOD_R RMOD
        LEFT JOIN CTP_COMMON_OBJECTS OBJ ON RMOD.OBJ_GID = OBJ.CO_GID
        WHERE RMOD.CONF_ID = 1
        """
results = connection.execute(stmt).fetchall()
RMOD = pd.DataFrame(results)
RMOD.columns = ['MRBTS','RMOD']
#RMOD['MRBTS'] = RMOD['MRBTS'].apply(lambda x: x.split(sep='/')[1].replace('MRBTS-', ''))
RMOD.MRBTS = RMOD.MRBTS.apply(lambda x: re.search("(?<=MRBTS-)(\d+)", x).group(1))

RMOD = RMOD\
    .fillna('X')\
    .groupby('MRBTS')['RMOD']\
    .apply(lambda x:
        "%s" % ' '.join(
                ['x'.join([str((x == i).sum()), i]
                ) for i in set(x)]))

RMOD = RMOD.reset_index(level=0)
print('RMOD done')

#GET RF SHARING
stmt =  """
        SELECT  OBJ.CO_DN, MNL.MNL_R_5R64499SRT
        FROM C_SRM_MNL_R MNL
        LEFT JOIN CTP_COMMON_OBJECTS OBJ ON MNL.OBJ_GID = OBJ.CO_GID
        WHERE MNL.CONF_ID = 1
        """
results = connection.execute(stmt).fetchall()
RFSH = pd.DataFrame(results)
RFSH.columns = ['MRBTS','Rfsharing']
#RFSH['MRBTS'] = RFSH['MRBTS'].apply(lambda x: x.split(sep='/')[1].replace('MRBTS-', ''))
RFSH.MRBTS = RFSH.MRBTS.apply(lambda x: re.search("(?<=MRBTS-)(\d+)", x).group(1))
RFSH.Rfsharing = RFSH.Rfsharing.map({0:'none',
                                     1:'UTRAN-EUTRA',
                                     2:'UTRAN-GERAN',
                                     3:'EUTRA-GERAN',
                                     4:'UTRAN-GERAN/CONCURRENT',
                                     5:'EUTRA-GERAN/CONCURRENT',
                                     6:'EUTRA-EUTRA',
                                     7:'EUTRA-EUTRA/EUTRA-GERAN',
                                     8:'EUTRA-CDMA'})
print('RFSH done')

# GET TXRX
stmt =  """
        SELECT OBJ.CO_DN, CH.CH_DIRECTION
        FROM C_SRM_CH CH
        LEFT JOIN CTP_COMMON_OBJECTS OBJ ON CH.OBJ_GID = OBJ.CO_GID
        WHERE CH.CONF_ID = 1
        """
results = connection.execute(stmt).fetchall()
TXRX = pd.DataFrame(results)
TXRX.columns = ['CELLID','TXRX']
TXRX.TXRX = TXRX.TXRX.map({1:'TX', 2:'RX'})
#TXRX.CELLID = TXRX.CELLID.apply(lambda x: x.split(sep='/')[1].replace('MRBTS-', '') + '-' + x.split(sep='/')[5].replace('LCELL-', ''))
TXRX.CELLID = TXRX.CELLID.apply(lambda x:
                                re.search("(?<=MRBTS-)(\d+)", x).group(1) +
                                '-' + 
                                re.search("(?<=LCELL-)(\d+)", x).group(1))

TXRX = TXRX\
    .groupby('CELLID')['TXRX']\
    .apply(lambda x:
        "%s" % ''.join(
                [''.join([str((x == i).sum()), i]
                ) for i in sorted(set(x), reverse=True)]))
TXRX = TXRX.reset_index(level=0)
print('TXRX done')

# GET LNCEL
stmt =  """
        SELECT OBJ.CO_DN AS DN,
        LNCEL.LNCEL_CELL_NAME AS CellName,
        LNCEL.LNCEL_EUTRA_CEL_ID AS eutraCelId,
        LNCEL.LNCEL_PHY_CELL_ID AS phyCellId,
        LNCEL_FDD.LNCEL_FDD_EARFCN_DL AS earfcnDL,
        LNCEL_FDD.LNCEL_FDD_DL_CH_BW/10 AS dlChBw,
        LNCEL.LNCEL_P_MAX AS pMax,
        LNCEL_FDD.LNCEL_FDD_DL_MIMO_MODE as dlMimoMode
        FROM C_LTE_LNCEL_FDD LNCEL_FDD
        LEFT JOIN CTP_COMMON_OBJECTS OBJ ON LNCEL_FDD.OBJ_GID = OBJ.CO_GID
        LEFT JOIN C_LTE_LNCEL LNCEL ON OBJ.CO_PARENT_GID = LNCEL.OBJ_GID
        WHERE LNCEL.CONF_ID = 1 AND LNCEL_FDD.CONF_ID = 1
        """
results = connection.execute(stmt).fetchall()
LNCEL = pd.DataFrame(results)
LNCEL.columns = ['DN','CellName','eutraCelId','phyCellId','earfcnDL','dlChBw','pMax','dlMimoMode']
#LNCEL.CELLID = LNCEL.CELLID.apply(lambda x: x.split(sep='/')[1].replace('MRBTS-', '') + '-' + x.split(sep='/')[3].replace('LNCEL-', ''))
LNCEL['CELLID'] = LNCEL.DN.apply(lambda x: re.search("(?<=MRBTS-)(\d+)", x).group(1) + '-' + re.search("(?<=LNCEL-)(\d+)", x).group(1))
LNCEL['MRBTS'] = LNCEL.DN.apply(lambda x: re.search("(?<=MRBTS-)(\d+)", x).group(1))
LNCEL['ID'] = LNCEL.DN.apply(lambda x: re.search("(?<=LNCEL-)(\d+)", x).group(1)).astype(int)

LNCEL['Site'] = LNCEL.CellName.str[1:9]
LNCEL.earfcnDL = LNCEL.earfcnDL.astype(str)
LNCEL.dlChBw = LNCEL.dlChBw.astype(str)
LNCEL['FreqBW'] = LNCEL.earfcnDL + '/' + LNCEL.dlChBw
LNCEL['Layer'] = pd.cut(LNCEL.ID, range(0,111,10))
LNCEL['Layer'].cat.categories = ['L2100','L1900','L700','L2100-3','NA1','NA2','L600','NA3','NA4','NA5','L2100-2']

LNCEL.drop(['DN', 'earfcnDL', 'dlChBw'],axis=1, inplace=True)

LNCEL.dlMimoMode = LNCEL.dlMimoMode.map({0:'SingleTX',\
                                         10:'TXDiv',\
                                         11:'4-way TXDiv',\
                                         30:'Dynamic Open Loop MIMO',\
                                         40:'Closed Loop Mimo',\
                                         41:'Closed Loop MIMO (4x2)',\
                                         43:'Closed Loop MIMO (4x4)' })

LNCEL = LNCEL[['CELLID','ID','Layer','MRBTS','Site','CellName','eutraCelId','FreqBW','phyCellId','pMax','dlMimoMode']]

print('LNCEL done')

#FINAL DF
#result = pd.concat([LNCEL, TXRX], axis=1)
result = pd.merge(LNCEL, TXRX, on='CELLID')
result = pd.merge(result, RMOD, on='MRBTS')
result = pd.merge(result, RFSH, on='MRBTS')
print('Save? ', end='')
a = input()
if a == 'y':
    now = datetime.datetime.now()
    result.to_csv(str(now).split()[0] + "_" + "CFG_QUERRY.csv")
    print('Saved')

query(result)