# -*- coding: utf-8 -*-
from sqlalchemy import create_engine
import pandas as pd
import datetime, re, os

def query(df, df3G):
    gp = df.groupby('Site')
    gp3G = df3G.groupby('Site')

    while True:
        site = input('Querry? ')
        if site == 'n' or '': break

        gp\
        .get_group(site)['IP']\
        .drop_duplicates()\
        .append(gp3G.get_group(site)['wbts_btsip_address'].drop_duplicates())\
        .to_clipboard(index=False)
 
        input("IPs copied\nPress Enter to continue...")

        gp\
            .get_group(site)\
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
        
        input("Hardware check copied\nPress Enter to continue...")
        
        tmpLTE = gp.get_group(site)[['MRBTS','Site','CellName','eutraCelId','FreqBW','phyCellId','administrativeState','cellBarred','primPlmnCellres']]
        tmp3G = gp3G.get_group(site)[['co_dn','Site','co_name','wcel_c_id','wcel_uarfcn','wcel_pri_scr_code','wcel_acs_31','wcel_cell_barred','wcel_cell_reserved']]
        
        tmpLTE\
        .rename(columns={'MRBTS':'MRBTS/WBTS',
                         'Site':'Site Name',
                         'CellName':'Sector Name',
                         'eutraCelId':'Cell ID',
                         'FreqBW':'Freq/BW',
                         'phyCellId':'PCI/PSC'},
                inplace=True)
        tmp3G\
        .rename(columns={'co_dn':'MRBTS/WBTS',
                         'Site':'Site Name',
                         'co_name':'Sector Name',
                         'wcel_c_id':'Cell ID',
                         'wcel_uarfcn':'Freq/BW',
                         'wcel_pri_scr_code':'PCI/PSC',
                         'wcel_acs_31':'administrativeState',
                         'wcel_cell_barred':'cellBarred',
                         'wcel_cell_reserved':'primPlmnCellres'},
                inplace=True)
        tmpLTE.append(tmp3G).to_clipboard(index=False)
        print('DT data copied')


def fetch_data(statement):
    df = pd.DataFrame(connection.execute(statement).fetchall())
    return df
    
def getIPNO():
    df = fetch_data("""
                      SELECT OBJ.CO_DN, IPNO_MPIA_8
                      FROM C_LTE_IPNO IPNO
                      LEFT JOIN CTP_COMMON_OBJECTS OBJ ON IPNO.OBJ_GID = OBJ.CO_GID
                      WHERE IPNO.CONF_ID = 1
                      """)
    df.columns = ['MRBTS','IP']
    df.MRBTS = df.MRBTS.apply(lambda x: re.search("(?<=MRBTS-)(\d+)", x).group(1))
    print('IPNO done')
    return df


def getRMOD():
    df = fetch_data("""
                      SELECT OBJ.CO_DN, RMOD_R_PRODUCT_NAME
                      FROM C_SRER_RMOD_R RMOD
                      LEFT JOIN CTP_COMMON_OBJECTS OBJ ON RMOD.OBJ_GID = OBJ.CO_GID
                      WHERE RMOD.CONF_ID = 1
                      """)
    df.columns = ['MRBTS','RMOD']
    df.MRBTS = df.MRBTS.apply(lambda x: re.search("(?<=MRBTS-)(\d+)", x).group(1))
    
    df = df\
        .fillna('X')\
        .groupby('MRBTS')['RMOD']\
        .apply(lambda x:
            "%s" % ' '.join(
                    ['x'.join([str((x == i).sum()), i]
                    ) for i in set(x)]))
    
    df = df.reset_index(level=0)
    print('RMOD done')
    return df

def getRFSH():
    df = fetch_data("""
                      SELECT  OBJ.CO_DN, MNL.MNL_R_5R64499SRT
                      FROM C_SRM_MNL_R MNL
                      LEFT JOIN CTP_COMMON_OBJECTS OBJ ON MNL.OBJ_GID = OBJ.CO_GID
                      WHERE MNL.CONF_ID = 1
                      """)
    df.columns = ['MRBTS','Rfsharing']
    df.MRBTS = df.MRBTS.apply(lambda x: re.search("(?<=MRBTS-)(\d+)", x).group(1))
    df.Rfsharing = df.Rfsharing.map({0:'none',
                                         1:'UTRAN-EUTRA',
                                         2:'UTRAN-GERAN',
                                         3:'EUTRA-GERAN',
                                         4:'UTRAN-GERAN/CONCURRENT',
                                         5:'EUTRA-GERAN/CONCURRENT',
                                         6:'EUTRA-EUTRA',
                                         7:'EUTRA-EUTRA/EUTRA-GERAN',
                                         8:'EUTRA-CDMA'})
    print('RFSH done')
    return df

def getTXRX():
    df = fetch_data("""
                      SELECT OBJ.CO_DN, CH.CH_DIRECTION
                      FROM C_SRM_CH CH
                      LEFT JOIN CTP_COMMON_OBJECTS OBJ ON CH.OBJ_GID = OBJ.CO_GID
                      WHERE CH.CONF_ID = 1
                      """)
    df.columns = ['CELLID','TXRX']
    df.TXRX = df.TXRX.map({1:'TX', 2:'RX'})
    #TXRX.CELLID = TXRX.CELLID.apply(lambda x: x.split(sep='/')[1].replace('MRBTS-', '') + '-' + x.split(sep='/')[5].replace('LCELL-', ''))
    df.CELLID = df.CELLID.apply(lambda x:
                                    re.search("(?<=MRBTS-)(\d+)", x).group(1) +
                                    '-' + 
                                    re.search("(?<=LCELL-)(\d+)", x).group(1))
    
    df = df\
        .groupby('CELLID')['TXRX']\
        .apply(lambda x:
            "%s" % ''.join(
                    [''.join([str((x == i).sum()), i]
                    ) for i in sorted(set(x), reverse=True)]))
    df = df.reset_index(level=0)
    print('TXRX done')
    return df

def getLNCEL():
    df = fetch_data("""
                    SELECT OBJ.CO_DN AS DN,
                    LNCEL.LNCEL_CELL_NAME AS CellName,
                    LNCEL.LNCEL_EUTRA_CEL_ID AS eutraCelId,
                    LNCEL.LNCEL_PHY_CELL_ID AS phyCellId,
                    LNCEL_FDD.LNCEL_FDD_EARFCN_DL AS earfcnDL,
                    LNCEL_FDD.LNCEL_FDD_DL_CH_BW/10 AS dlChBw,
                    LNCEL.LNCEL_P_MAX AS pMax,
                    LNCEL_FDD.LNCEL_FDD_DL_MIMO_MODE as dlMimoMode,
                    LNCEL.LNCEL_AS_26 AS administrativeState,
                    LNCEL_SIB.BARRED AS cellBarred,
                    LNCEL_SIB.RESERVED AS primPlmnCellres                       
                    FROM C_LTE_LNCEL_FDD LNCEL_FDD
                    LEFT JOIN CTP_COMMON_OBJECTS OBJ ON LNCEL_FDD.OBJ_GID = OBJ.CO_GID
                    LEFT JOIN C_LTE_LNCEL LNCEL ON OBJ.CO_PARENT_GID = LNCEL.OBJ_GID
                    LEFT JOIN
                           (SELECT OBJ1.CO_PARENT_GID SIB_PARENT, SIB_CLL_BARRED BARRED, SIB_PPCR_55 RESERVED
                            FROM C_LTE_SIB SIB
                            LEFT JOIN CTP_COMMON_OBJECTS OBJ1 ON SIB.OBJ_GID = OBJ1.CO_GID
                            WHERE SIB.CONF_ID = 1
                            ) LNCEL_SIB ON LNCEL_SIB.SIB_PARENT = LNCEL.OBJ_GID
                                          
                    WHERE LNCEL.CONF_ID = 1 AND LNCEL_FDD.CONF_ID = 1
                    """)
    df.columns = ['DN','CellName','eutraCelId','phyCellId','earfcnDL','dlChBw','pMax','dlMimoMode','administrativeState','cellBarred','primPlmnCellres']
    df['CELLID'] = df.DN.apply(lambda x:
                                   re.search("(?<=MRBTS-)(\d+)", x).group(1) + 
                                   '-' + 
                                   re.search("(?<=LNCEL-)(\d+)", x).group(1))
    df['MRBTS'] = df.DN.apply(lambda x: re.search("(?<=MRBTS-)(\d+)", x).group(1))
    df['ID'] = df.DN.apply(lambda x: re.search("(?<=LNCEL-)(\d+)", x).group(1)).astype(int)
    
    df['Site'] = df.CellName.str[1:9]
    df.earfcnDL = df.earfcnDL.astype(str)
    df.dlChBw = df.dlChBw.astype(str)
    df['FreqBW'] = df.earfcnDL + '/' + df.dlChBw
    df['Layer'] = pd.cut(df.ID, range(0,111,10))
    df['Layer'].cat.categories = ['L2100','L1900','L700','L2100-3','NA1','NA2','L600','NA3','NA4','NA5','L2100-2']
    
    df.drop(['DN', 'earfcnDL', 'dlChBw'],axis=1, inplace=True)
    
    df.dlMimoMode = df.dlMimoMode.map({0:'SingleTX',\
                                             10:'TXDiv',\
                                             11:'4-way TXDiv',\
                                             30:'Dynamic Open Loop MIMO',\
                                             40:'Closed Loop Mimo',\
                                             41:'Closed Loop MIMO (4x2)',\
                                             43:'Closed Loop MIMO (4x4)' })
    df.administrativeState = df.administrativeState.map({1:'unlocked',
                                                         2:'shutting down',
                                                         3:'locked'})
    df.cellBarred = df.cellBarred.map({0:'barred',
                                       1:'notBarred'})
    
    df.primPlmnCellres = df.primPlmnCellres.map({0:'Not Reserved',
                                                 1:'Reserved'})
    
    df = df[['CELLID','ID','Layer','MRBTS','Site','CellName','eutraCelId','FreqBW','phyCellId','pMax','dlMimoMode','administrativeState','cellBarred','primPlmnCellres']]
    
    print('LNCEL done')
    return df

def get3G():
    df = fetch_data("""
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
                    """)
        
    df.columns = ['co_dn','co_name','wcel_c_id','wcel_uarfcn','wcel_pri_scr_code','wcel_acs_31','wcel_cell_barred','wcel_cell_reserved','wbts_btsip_address']
    df.co_dn = df.co_dn.apply(lambda x: re.search("(?<=WBTS-)(\d+)", x).group(1))
    df['Site'] = df.co_name.str[1:9]
    df.wcel_acs_31 = df.wcel_acs_31.map({0:'Locked',
                                         1:'Unlocked'})
    
    df.wcel_cell_barred = df.wcel_cell_barred.map({0:'Barred',
                                                   1:'Not barred'})
    
    df.wcel_cell_reserved = df.wcel_cell_reserved.map({0:'Reserved',
                                                       1:'Not reserved'})
      
    df = df[['co_dn','Site','co_name','wcel_c_id','wcel_uarfcn','wcel_pri_scr_code','wcel_acs_31','wcel_cell_barred','wcel_cell_reserved','wbts_btsip_address']]
  
    print('3G done')
    return df

def local_df(tech, path, query_date):
    
    curdir = os.listdir(path)
    date = datetime.datetime.strptime('2018-01-01', "%Y-%m-%d")
    if tech == 'LTE': filt = 'LTE_CFG_QUERRY'
    elif tech == '3G': filt = '3G_CFG_QUERRY'
    
    if query_date == 'new':
        for f in curdir:
            if filt in f:
                tmp = datetime.datetime.strptime(re.search("^\d+-\d+-\d+", f).group(0), "%Y-%m-%d")
                if tmp > date:
                    date = tmp
                    file = f
    elif query_date == 'old':
        file_dict = {i:f for i,f in enumerate(curdir) if filt in f}
        print('Old ' + tech + ' dumps: ')
        for i in file_dict.keys():
            print(i,': ', file_dict[i])
        a = input('Which one? ')
        file = file_dict[int(a)]
    
    print('Open file: ', file)
    df = pd.read_csv(path + file)
    return df


def main():
    mypath = r"C:\Python\git\OSS CM Query\output\\"
    a = input('Pull data from OSS? ')
    if a == 'n':
        a = input('(o)ld or (n)ew? ')
        if a == 'n':
            result = local_df('LTE', mypath, 'new')
            result3G = local_df('3G', mypath, 'new')
        elif a == 'o':
            result = local_df('LTE', mypath, 'old')
            result3G = local_df('3G', mypath, 'old')
    elif a == 'y':
        engine = create_engine('oracle://aurfeng:Default1@T4OSS')
        connection = engine.connect()
        
        result = pd.merge(getLNCEL(), getTXRX(), on='CELLID')
        result = pd.merge(result, getRMOD(), on='MRBTS')
        result = pd.merge(result, getRFSH(), on='MRBTS')
        result = pd.merge(result, getIPNO(), on='MRBTS')
        result3G = get3G()
        a = input('Save? ')
        if a == 'y':
            now = datetime.datetime.now()
            result.to_csv(mypath + str(now).split()[0] + "_" + "LTE_CFG_QUERRY.csv")
            result3G.to_csv(mypath + str(now).split()[0] + "_" + "3G_CFG_QUERRY.csv")
            print('Saved')
    else: print('error')
    query(result, result3G)
    
if __name__ == "__main__":
    main()