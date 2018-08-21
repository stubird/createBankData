import json
import sys
import os

def csvToJson(tableName, file, date_range = {}):
    if os.path.exists("./jsonFormat/{}.json".format(tableName)):
        print("write faild, file exist!")
        return

    if os.path.exists(file) is not True:
        print("config csv file is not exist!")
        return

    with open(file) as f_op:
        jsonStr = {
            "tables":{},
            "constraint":{}
            }
        currentTab = ""
        for line in f_op:
            line = line[:-1]
            line = line.split(',')

            if line[0] == "":
                continue

            if line[0] not in jsonStr["tables"]:
                #new table init
                currentTab = line[0]
                jsonStr["tables"][line[0]] = {}
                jsonStr["tables"][line[0]]["property"] = {"lines": "1000"}
                jsonStr["tables"][line[0]]["constraint"] = {}

            if "field" not in jsonStr["tables"][line[0]]:
                jsonStr["tables"][line[0]]["field"] = {}

            if "DTE" in line[1] or "日期" in line[3] or "DATE" in line[1]:
                jsonStr["tables"][line[0]]["field"][line[1]] = {
                    "type": "DATE",
                    "createMod": "",
                    "constraint": date_range
                }
            else:
                jsonStr["tables"][line[0]]["field"][line[1]] = {
                    "type":line[2],
                    "createMod": "",
                    "constraint": {}
                }



        with open("./jsonFormat/{}.json".format(tableName),'w+') as tj:
            json.dump(jsonStr,tj,indent=4)

if __name__ == "__main__":
    #csvToJson(sys.argv[1])
    #csvToJson("ACTINS","./tableCsv/ACTINS.csv")
    #csvToJson("CADACT", "./tableCsv/CADACT.csv")
    #csvToJson("CADTRN", "./tableCsv/CADTRN.csv")
    #csvToJson("CADINF_EOM","./tableCsv/CADINF_EOM.csv",{"floor":"2017-01-01","upper":"2017-12-31"})
    #csvToJson("CUSINF_EOM", "./tableCsv/CUSINF_EOM.csv", {"floor": "2017-01-01", "upper": "2017-12-31"})
    #csvToJson("ACTINF_EOM", "./tableCsv/ACTINF_EOM.csv", {"floor": "2017-01-01", "upper": "2017-12-31"})
    #csvToJson("UPDLMT", "./tableCsv/UPDLMT.csv", {"floor": "2017-01-01", "upper": "2017-12-31"})
    #csvToJson("CUSMSG_EOM", "./tableCsv/CUSMSG_EOM.csv", {"floor": "2017-01-01", "upper": "2017-12-31"})
    #csvToJson("PCUSTM_HEDR", "./tableCsv/PCUSTM_HEDR.csv", {"floor": "2017-01-01", "upper": "2017-12-31"})
    #csvToJson("PCUSTM_SHAR", "./tableCsv/PCUSTM_SHAR.csv", {"floor": "2017-01-01", "upper": "2017-12-31"})
    #csvToJson("PCUSTM_SHBR", "./tableCsv/PCUSTM_SHBR.csv", {"floor": "2017-01-01", "upper": "2017-12-31"})
    #csvToJson("PCUSTM_POSD", "./tableCsv/PCUSTM_POSD.csv", {"floor": "2017-01-01", "upper": "2017-12-31"})
    #csvToJson("CADDLY", "./tableCsv/CADDLY.csv", {"floor": "2017-01-01", "upper": "2017-12-31"})
    #csvToJson("PAY_HIS", "./tableCsv/PAY_HIS.csv", {"floor": "2017-01-01", "upper": "2017-12-31"})
    #csvToJson("FSC_PARTY_DIM", "./tableCsv/pengtxt", {"floor": "2017-01-01", "upper": "2017-12-31"})
    csvToJson("CUS_ASSET_EOM", "./tableCsv/CUS_ASSET_EOM.csv", {"floor": "2017-01-01", "upper": "2017-12-31"})
