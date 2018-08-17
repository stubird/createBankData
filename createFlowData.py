# coding: utf8
import random
from datetime import datetime
from datetime import timedelta
import logging
import os
import json

from pyspark.sql.functions import udf
from pyspark import SparkContext
from pyspark import SQLContext, HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types import *
import pyspark.sql.types as Types
import sys
import time

SEED_NUM = 10000
logging.basicConfig(format="%(levelname)s %(asctime)s [%(filename)s +%(lineno)s %(funcName)s] %(message)s",
                    level=logging.WARNING)
logger = logging.getLogger("4paradigm.com")
logger.setLevel(logging.DEBUG)

class excuteFunc(object):
    @classmethod
    def StringMethod(self, x = "", constraint = {}):
        if "strset" in constraint:
            defastr = constraint["strset"]
        else:
            defastr = ["ABC","DEF","GHI"]

        def method(x):
            random.seed(x * SEED_NUM + time.time())
            return defastr[random.randint(0, len(defastr) - 1)]

        return method

    @classmethod
    def IntegerMethod(self, x = "", constraint = {}):
        def method(x):
            random.seed(x * SEED_NUM + time.time())
            return random.randint(
                int(eval(constraint["floor"])) if "floor" in constraint else -(sys.maxsize-1)/2,
                int(eval(constraint["upper"])) if "upper" in constraint else (sys.maxsize-1)/2
            )
        return method

    @classmethod
    def FloatMethod(self, x = "", constraint = {}):
        def method(x):
            random.seed(x * SEED_NUM + time.time())
            floor = float(eval(constraint["floor"])) if "floor" in constraint else -(sys.maxsize-1)/2
            upper = float(eval(constraint["upper"])) if "upper" in constraint else (sys.maxsize-1)/2
            return floor + random.random() * (upper - floor)
        return method

    @classmethod
    def DateMethod(self, x = "", constraint = {}):
        if constraint["floor"] == "":
            floor = datetime.strptime("2000-01-01", "%Y-%m-%d")
        else:
            floor = datetime.strptime(constraint["floor"], "%Y-%m-%d")

        if constraint["upper"] == "":
            upper = datetime.strptime("2010-01-01", "%Y-%m-%d")
        else:
            upper = datetime.strptime(constraint["upper"], "%Y-%m-%d")

        def method(x):
            random.seed(x * SEED_NUM + time.time())
            timeoffset = timedelta(seconds=
                random.randint(0, int((upper - floor).total_seconds())))

            return (floor + timeoffset).date()
        return method

    @classmethod
    def DefaultMethod(self, x = "", constraint = {}):
        def method(x):
            random.seed(x*SEED_NUM+time.time())
            return constraint["strset"][random.randint(0,len(constraint["strset"])-1)] \
                if "strset" in constraint else ["ABC","DEF","GHI"][random.randint(0,2)]

        return method

    @classmethod
    def ID_Method(self, x = "", constraint = {}):
        if "lenOfId" in constraint:
            lenOfId = constraint["lenOfId"]
        else:
            lenOfId = 8

        def method(x):
            random.seed(x * SEED_NUM + time.time())
            IdStr = ""
            for i in range(lenOfId):
                IdStr += str(random.randint(0,9))
            return IdStr

        return method

    @classmethod
    def DATEUSER_Method(self, x = "", constraint = {}):
        if constraint["floor"] == "":
            floor = datetime.strptime("2000-01-01", "%Y-%m-%d")
        else:
            floor = datetime.strptime(constraint["floor"], "%Y-%m-%d")

        if constraint["upper"] == "":
            upper = datetime.strptime("2010-01-01", "%Y-%m-%d")
        else:
            upper = datetime.strptime(constraint["upper"], "%Y-%m-%d")

        def method(x):
            random.seed(x * SEED_NUM + time.time())
            timeoffset = timedelta(seconds=
                random.randint(0, int((upper - floor).total_seconds())))

            if constraint["datatype"].upper() == "DATE":
                return floor + timeoffset
            else:
                return str(floor + timeoffset)

        return method



def run(t1, context_string, configPath = './jsonFormat/ACTCLR.json'):
    spark = SQLContext(SparkContext.getOrCreate())

    hive = HiveContext(SparkContext.getOrCreate())

    jsonobj = {}

    #load json obj from file
    with open(configPath) as cfgPath:
        jsonobj = json.load(cfgPath)

    print("jsonobj",jsonobj)
    tables = {}

    for name,prof in jsonobj["tables"].items():
        #parse table
        print("prof",name,prof)

        linenum = prof["property"]["lines"]
        df = spark.range(linenum)

        for fieldName,fieldProf in prof["field"].items():
            if fieldProf["type"].startswith("\"") and fieldProf["type"].endswith("\""):
                fieldProf["type"] = fieldProf["type"][1:-1]
            if fieldProf["type"].upper().startswith("VARCHAR") or \
                fieldProf["type"].upper().startswith("CHAR"):
                t_type = StringType
                baseType = "String"
            elif fieldProf["type"].upper().startswith("DECIMAL"):
                t_type = FloatType
                baseType = "Float"
            elif fieldProf["type"].upper().startswith("SMALLINT"):
                t_type = IntegerType
                baseType = "Integer"
            elif fieldProf["type"].upper().startswith("DATE"):
                t_type = DataType
                baseType = "Date"
            else:
                t_type = StringType
                baseType = "String"

            if fieldProf["createMod"] == "":
                udf_func = udf(getattr(excuteFunc,baseType+"Method")(constraint = fieldProf["constraint"]), t_type())
            else:
                udf_func = udf(getattr(excuteFunc, fieldProf["createMod"].upper()+"_Method")(constraint = fieldProf["constraint"]), t_type())

            df = df.withColumn(fieldName, udf_func("id"))

        tables[name] = df

    for i,j in tables.items():
        j.show(n=100,truncate=False)

    return [tables["ACTCLR"]]

if __name__ == '__main__':
    ret = run("","")