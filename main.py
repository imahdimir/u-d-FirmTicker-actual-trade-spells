"""

  """

import json

import pandas as pd
from githubdata import GithubData
from mirutil.df_utils import save_as_prq_wo_index as sprq
from mirutil.jdate import vect_make_datetime_from_iso_jdate_time as vjd
from persiantools.jdatetime import JalaliDate


class GDUrl :
    with open('gdu.json' , 'r') as fi :
        gj = json.load(fi)

    selff = gj['selff']
    src0 = gj['src0']
    src1 = gj['src1']
    trg = gj['trg']

gu = GDUrl()

class ColNames :
    d = 'Date'
    jdt = 'JDateTime'
    dt = 'DateTime'
    sjdt = 'StartJDateTime'
    ejdt = 'EndJDateTime'
    ns = 'NewStatus'
    trdble = 'Tradable'
    ismktopen = 'IsMarketOpen'
    sdt = 'StartDateTime'
    edt = 'EndDateTime'
    ndt = 'NextDateTime'
    dur = 'Duration'
    ftic = 'FirmTicker'
    jd = 'JDate'

c = ColNames()

class Status :
    tradeable = True
    not_tradeable = False

s = Status()

status_simplified = {
        'مجاز'        : s.tradeable ,
        'مجاز-محفوظ'  : s.not_tradeable ,
        'مجاز-متوقف'  : s.not_tradeable ,
        'ممنوع-متوقف' : s.not_tradeable ,
        'ممنوع'       : s.not_tradeable ,
        'ممنوع-محفوظ' : s.not_tradeable ,
        'مجاز-مسدود'  : s.not_tradeable ,
        'ممنوع-مسدود' : s.not_tradeable ,
        }

def main() :
    pass

    ##

    gs0 = GithubData(gu.src0)
    gs0.overwriting_clone()
    ##
    ds = gs0.read_data()
    ##
    ds[c.trdble] = ds[c.ns].map(status_simplified)
    ##
    ds = ds[[c.ftic , c.jdt , c.trdble]]
    ##
    ds[c.dt] = ds[c.jdt].apply(vjd)
    ds = ds.drop(columns = c.jdt)
    ##

    gs1 = GithubData(gu.src1)
    gs1.overwriting_clone()
    d1 = gs1.read_data()
    d1.head()
    ##

    d1[c.sdt] = d1[c.sjdt].apply(vjd)
    d1[c.edt] = d1[c.ejdt].apply(vjd)

    d1 = d1.drop(columns = [c.sjdt , c.ejdt])

    d0v = d1.head()
    ##

    ds[c.d] = ds[c.dt].dt.date
    ##
    d1[c.d] = d1[c.sdt].dt.date
    ##

    ds = ds.merge(d1 , on = [c.ftic , c.d] , how = 'left')
    ##
    ds[c.ismktopen] = ds[c.dt].ge(ds[c.sdt])
    ds[c.ismktopen] &= ds[c.dt].le(ds[c.edt])

    ##
    fu1 = lambda x : x.any()
    by = [c.ftic , c.dt]
    ds[c.ismktopen] = ds.groupby(by)[c.ismktopen].transform(fu1)

    ##
    ds = ds.drop(columns = [c.sdt , c.edt , c.d])

    ##

    d1 = d1[[c.ftic]]
    d1[c.dt] = d1[c.sdt]

    d2 = d1[[c.ftic]]
    d2[c.dt] = d1[[c.edt]]
    ##
    d3 = pd.concat([d1 , d2])
    ##
    del d1 , d2
    ##

    d3[c.ismktopen] = True
    ##
    d3.head()

    ##

    df = d3.merge(ds , on = [c.ftic , c.dt] , how = 'outer')
    ##
    dfv = df.head()
    ##
    del d3
    ##

    msk = df.duplicated(subset = [c.ftic , c.dt] , keep = False)
    msk &= df[c.trdble].isna()

    df1 = df[msk]
    df = df[~ msk]
    ##

    msk = df[c.ismktopen + '_x'].isna()
    print(len(msk[msk]))

    df.loc[msk , c.ismktopen + '_x'] = df[c.ismktopen + '_y']

    ##
    df = df.drop(columns = c.ismktopen + '_y')

    ren = {
            c.ismktopen + '_x' : c.ismktopen ,
            }
    df = df.rename(columns = ren)
    ##
    dfv = df.head()
    ##

    df = df.sort_values(by = [c.ftic , c.dt])
    dfv = df.head()
    ##
    df[c.trdble] = df.groupby([c.ftic])[c.trdble].ffill()

    ##

    msk = df[c.ismktopen]
    msk &= df[c.trdble].notna()

    df1 = df[~ msk]
    df = df[msk]

    ##

    df = df.drop(columns = c.ismktopen)
    ##

    df[c.d] = pd.to_datetime(df[c.dt]).dt.date
    ##
    df[c.ndt] = df.groupby([c.ftic , c.d])[c.dt].shift(-1)
    dfv = df.head()

    ##

    ncol = pd.to_datetime(df[c.ndt]) - pd.to_datetime(df[c.dt])
    ncol = ncol.dt.seconds
    ##
    msk = df[c.trdble]
    df.loc[msk , c.dur] = ncol[msk]

    dfv = df.head()
    ##

    df1 = df.groupby([c.ftic , c.d])[c.dur].sum()
    ##
    df1 = df1.to_frame()
    df1 = df1.reset_index()
    ##
    df1[c.dur] = df1[c.dur].astype(int)
    ##
    df1v = df1.head(1000)
    ##

    df1[c.jd] = df1[c.d].apply(lambda x : JalaliDate(x).isoformat())

    ##
    df1 = df1[[c.ftic , c.jd , c.dur]]
    df1v = df1.head(1000)

    ##

    gdt = GithubData(gu.trg)
    gdt.overwriting_clone()

    ##
    sprq(df1 , gdt.data_fp)

    ##
    msg = 'builded by: '
    msg += gu.selff
    ##

    gdt.commit_and_push(msg)

    ##

    gs0.rmdir()
    gs1.rmdir()
    gdt.rmdir()

    ##

##
if __name__ == '__main__' :
    main()
    print('done')
