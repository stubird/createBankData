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

logging.basicConfig(format="%(levelname)s %(asctime)s [%(filename)s +%(lineno)s %(funcName)s] %(message)s",
                    level=logging.WARNING)
logger = logging.getLogger("4paradigm.com")
logger.setLevel(logging.DEBUG)

class excuteFunc(object):
    @classmethod
    def StringMethod(self, x = "", constraint = {}):
        def method(x):
            return constraint["strset"][random.randint(0,len(constraint["strset"])-1)] \
                if "strset" in constraint else ["ABC","DEF","GHI"][random.randint(0,2)]

        return method

    @classmethod
    def IntegerMethod(self, x = "", constraint = {}):
        def method(x):
            return random.randint(
                int(eval(constraint["floor"])) if "floor" in constraint else -(sys.maxsize-1)/2,
                int(eval(constraint["upper"])) if "upper" in constraint else (sys.maxsize-1)/2
            )
        return method

    @classmethod
    def DoubleMethod(self, x = "", constraint = {}):
        def method(x):
            floor = float(eval(constraint["floor"])) if "floor" in constraint else -(sys.maxsize-1)/2
            upper = float(eval(constraint["upper"])) if "upper" in constraint else (sys.maxsize-1)/2
            return floor + random.random() * (upper - floor)
        return method

    @classmethod
    def DateMethod(self, x = "", constraint = {}):
        def method(x):
            basedate = datetime.strptime(
                constraint["basedate"] if "basedate" in constraint else "2000-01-01", "%Y-%m-%d")

            timeoffset = timedelta(seconds=
                random.randint(0, eval(constraint["offset"])) if "offset"
                    in constraint else random.randint(0,86400*(365*18+4)))

            return (basedate + timeoffset).date()
        return method

    @classmethod
    def DefaultMethod(self, x = "", constraint = {}):
        return "AA"

def run(t1, context_string, configPath = './jsonFormat/configTables.json'):
    spark = SQLContext(SparkContext.getOrCreate())

    hive = HiveContext(SparkContext.getOrCreate())

    t1_cust_id = "CustId"
    t1_prod_id = "ProdId"
    t1_trx_dt = "TrxDt"
    t1_trx_code = "TrxCode"

    logger.info("transform instances to spark dataframe starts")
    schema = StructType([
        StructField(t1_cust_id, StringType(), True),
        StructField(t1_prod_id, StringType(), True),
        StructField(t1_trx_dt, DateType(), True),
        StructField(t1_trx_code, IntegerType(), True)
    ])

    func_str2date = lambda x: datetime.datetime.strptime(x, "%Y-%m-%d").date()


    jsonobj = {}

    getattr(Types, "StringType")()

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
            t_type = getattr(Types, fieldProf["type"]+"Type")
            if fieldProf["createMod"] == "":
                udf_func = udf(getattr(excuteFunc,fieldProf["type"]+"Method")(constraint = fieldProf["constraint"]), t_type())
            else:
                udf_func = udf(getattr(excuteFunc, fieldProf["createMod"]), t_type())

            df = df.withColumn(fieldName, udf_func("id"))

        tables[name] = df

    tables["table1"].show()
    tables["table2"].show()
    return [tables["table1"]]
"""
    instances = []
    instances2 = []
    for i in range(20):
        instances.append(("1", func_str2date("2018-01-01"), "fe"))
    for i in range(20):
        instances2.append(("1", 23.12, "42fd"))

    df = spark.createDataFrame(instances, tables["table1"])
    df2 = spark.createDataFrame(instances2, tables["table2"])

    df.show()
    df2.show()
"""


if __name__ == '__main__':
    ret = run("","")
    #ret[0].show()