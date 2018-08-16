import json
import sys

def csvToJson(file = "./tableCsv/tmp.csv"):
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
                jsonStr["tables"][line[0]]["property"] = {"lines": 50}
                jsonStr["tables"][line[0]]["constraint"] = {}

            if "field" not in jsonStr["tables"][line[0]]:
                jsonStr["tables"][line[0]]["field"] = {}

            jsonStr["tables"][line[0]]["field"][line[1]] = {"type": line[2],
                    "createMod":"",
                    "constraint":""
                }


        with open("./jsonFormat/ACTCLR11.json",'w+') as tj:
            json.dump(jsonStr,tj,indent=4)

if __name__ == "__main__":
    #csvToJson(sys.argv[1])
    csvToJson()