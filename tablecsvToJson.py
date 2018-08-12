import json
import sys

def csvToJson(file = "./tableCsv/tables.csv"):
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
                jsonStr["tables"][line[0]]["field"] = {}
                jsonStr["tables"][line[0]]["property"] = {"lines": 50}
                jsonStr["tables"][line[0]]["constraint"] = {}

            tablename = line[0]
            field = line[1]
            ftype = line[2]

            assert (tablename != currentTab and tablename in jsonStr["tables"]) == False,"Error: table already exist!"
            assert field not in jsonStr["tables"][tablename]["field"], "Error: table:{} field:{} already exist!".format(line[0],line[1])

            if len(line) > 3:
                consplit = line[3].split("~")
                floor = consplit[0]
                upper = consplit[1]
                constraint = {"floor":floor,"upper":upper}
            else:
                constraint = ""

            jsonStr["tables"][tablename]["field"][field] = {"type": ftype,
                        "createMod":"",
                        "constraint":constraint
                    }

            print(line)
        with open("./jsonFormat/tables.json",'w+') as tj:
            json.dump(jsonStr,tj,indent=4)

if __name__ == "__main__":
    #csvToJson(sys.argv[1])
    csvToJson()