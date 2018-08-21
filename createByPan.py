import pandas as pd
import random
import time
import numpy as np
import sys
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import os
import json

def UserId_Method(lens = 10, num = 100):
    idSet = []
    #random.seed(time.time())
    IdStr = "".join([str(random.randint(0, 9)) for i in range(lens)])

    for _ in range(num):
        while IdStr in idSet:
            IdStr = ""
            for i in range(lens):
                IdStr += str(random.randint(0, 9))
        idSet.append(IdStr)
    return idSet

def Name_Method(lens = 4, num = 100):
    idSet = []
    #random.seed(time.time())

    IdStr = "".join([chr(random.randint(65, 90)) for i in range(lens)])

    for _ in range(num):
        while IdStr in idSet:
            IdStr = ""
            for i in range(lens):
                IdStr += chr(random.randint(65, 90))
        idSet.append(IdStr)
    return idSet

def DISCRET_Method(constraint = {},num = 100):
    idSet = []
    if "strset" in constraint:
        strset = constraint["strset"]
    else:
        strset = ["IDCARD", "DRIVERCARD", "CREDCARD"]
    # random.seed(time.time())

    for _ in range(num):
        idSet.append(strset[random.randint(0, len(strset)-1)])

    return idSet

def IntegerMethod(constraint = {},num = 100):
    rets = []
    for _ in range(num):
        rets.append(
                random.randint(
                    int(eval(constraint["floor"])) if "floor" in constraint else 0,
                    int(eval(constraint["upper"])) if "upper" in constraint else 100
            )
        )
    return rets

def FloatMethod(constraint = {},num = 100):

    rets = []
    for _ in range(num):
        floor = float(eval(constraint["floor"])) if "floor" in constraint else 0
        upper = float(eval(constraint["upper"])) if "upper" in constraint else 200000
        rets.append(floor + random.random() * (upper - floor))

    return rets

def DateMethod(constraint = {},num = 100):
    if "floor" not in constraint:
        floor = datetime.strptime("2000-01-01", "%Y-%m-%d")
    else:
        floor = datetime.strptime(constraint["floor"], "%Y-%m-%d")

    if "upper" not in constraint:
        upper = datetime.strptime("2010-01-01", "%Y-%m-%d")
    else:
        upper = datetime.strptime(constraint["upper"], "%Y-%m-%d")

    rets = []
    for _ in range(num):
        timeoffset = timedelta(seconds=
            random.randint(0, int((upper - floor).total_seconds())))
        rets.append((floor + timeoffset).date())
    return rets

def createTable(jsonobj, initDf = None,store = None):

    pd.set_option('max_rows', 1000)
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_columns', 1000)

    for name,prof in jsonobj["tables"].items():
        #parse table
        #print("prof",name,prof)

        linenum = eval(prof["property"]["lines"])

        if initDf is not None:
            df = initDf.loc[:]
        else:
            df = pd.DataFrame()

        for fieldName,fieldProf in prof["field"].items():
            if fieldProf["type"].startswith("\"") and fieldProf["type"].endswith("\""):
                fieldProf["type"] = fieldProf["type"][1:-1]

            if fieldName in df:
                continue
            #particular function
            if fieldProf["createMod"].upper() == "ID":
                if "length" in fieldProf["constraint"]:
                    length = eval(fieldProf["constraint"]["length"])
                else:
                    length = 10

                df[fieldName] = UserId_Method(lens=length, num=linenum)
                continue

            if fieldProf["createMod"].upper() == "NAME":
                df[fieldName] = Name_Method(fieldProf["constraint"],num=linenum)
                continue

            if fieldProf["createMod"].upper() == "CARDTYPE":
                df[fieldName] = DISCRET_Method(fieldProf["constraint"],num=linenum)
                continue

            if fieldProf["createMod"].upper() == "CARDID":
                df[fieldName] = UserId_Method(18,num=linenum)
                continue

            if fieldProf["createMod"].upper().startswith("DATE"):
                df[fieldName] = DateMethod(fieldProf["constraint"],num=linenum)
                continue

            if fieldProf["type"].upper().startswith("VARCHAR") or \
                fieldProf["type"].upper().startswith("CHAR"):
                namelen = eval(fieldProf["constraint"]["length"]) if "length" in fieldProf["constraint"] else 5
                df[fieldName] = Name_Method(namelen,num=linenum)
            elif fieldProf["type"].upper().startswith("DECIMAL") or fieldProf["type"].upper().startswith("\"DECIMAL") or\
                fieldProf["type"].upper().startswith("NUMBER") or fieldProf["type"].upper().startswith("\"NUMBER"):
                df[fieldName] = FloatMethod(fieldProf["constraint"],num=linenum)
            elif fieldProf["type"].upper().startswith("SMALLINT") or "INTEGER" in fieldProf["type"].upper():
                df[fieldName] = IntegerMethod(fieldProf["constraint"],num=linenum)
            elif fieldProf["type"].upper().startswith("DATE"):
                df[fieldName] = DateMethod(fieldProf["constraint"],num=linenum)
            else:
                df[fieldName] = Name_Method(5,num=linenum)

        #print(df)
        if store is not None:
            df.to_csv(store, index=False)
        return df


def createMonthSlice(table, fielist, dateCol, datarecord = "DataTime",initdf = None, time_gap = ["2017-01","2017-12"]):
    # load json obj from file
    with open("./jsonFormat/{}.json".format(table)) as cfgPath:
        jsonobj = json.load(cfgPath)

    if initdf is not None:
        df1 = initdf
    else:
        df1 = createTable(jsonobj)

    dfs = []

    start = datetime.strptime(time_gap[0], "%Y-%m")
    end = datetime.strptime(time_gap[1], "%Y-%m")

    loops = 12*end.year + end.month - 12*start.year - start.month + 1

    print("loops:",loops,"start:",start,"end:",end)

    # add data record time
    jsonobj["tables"][table]["field"][datarecord] = {
                    "type": "VARCHAR(70)",
                    "createMod": "CARDTYPE",
                    "constraint": {}
                }

    for i in range(loops):

        floor = str((start + relativedelta(months=i)).date())
        upper = str((start + relativedelta(months=i+1,days=-1)).date())

        #print("i:{},upper:{},floor:{}".format(i,upper,floor))

        jsonobj["tables"][table]["field"][dateCol]["constraint"] = {"floor": floor, "upper": upper}
        jsonobj["tables"][table]["field"][datarecord]["constraint"]["strset"] = [floor[:7]]

        dfs.append(createTable(jsonobj, df1.loc[:, fielist]))

    ret = dfs[0]

    print("len ret ",len(ret),"dfs:",len(dfs))
    for i in dfs[1:]:
        #print(i)
        #print("i",len(i))
        ret = ret.append(i)

    print("len2 ret ",len(ret))
    return ret

if __name__ == '__main__':

    """
    #card and person relationship
    with open("./jsonFormat/CADACT.json") as CADACT:
        jsonobj = json.load(CADACT)
        df = createTable(jsonobj)
        print(df)
        if os.path.exists("./datastore/CADACT.csv") is not True:
            with open("./datastore/CADACT.csv","w+") as CADACT_DATA:
                df.to_csv(CADACT_DATA,index=False)

    """

    with open("./jsonFormat/FSC_PARTY_DIM.json") as CADACT:
        jsonobj = json.load(CADACT)
        createTable(jsonobj,store="./datastore/FSC_PARTY_DIM.csv")
    pass

    """
    #ACTCLR

    df_ACTCLR = df.loc[:,["ACT_IDN_SKY","SCL_SCY_NBR_TXT","CSR_IDN_SKY","CSR_NME"]]

    df_ACTCLR.rename(columns={"SCL_SCY_NBR_TXT":"SCL_SCY_NBR"}, inplace=True)

    df_ACTCLR = createMonthSlice("ACTCLR",["ACT_IDN_SKY","SCL_SCY_NBR","CSR_IDN_SKY","CSR_NME"],"DQT_STT_DTE",initdf=df_ACTCLR)

    with open("./datastore/ACTCLR.csv", 'w+') as ACT:
        df_ACTCLR.to_csv(ACT,index=False)
    """

    #CADTRN


