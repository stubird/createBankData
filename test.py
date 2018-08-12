import json
import os,sys

def findplay():
    print("gogo")

sys.path.append('../')

from createBankData import test


def theGood1():
    array = ["I","AM","a","good","man"]
    def method():
        print(array[0])
        del array[0]
        print("wawawa12 aaaa")
    return  method

class aaa(object):
    @classmethod
    def theGood(self):
        print("wawawa aaaa")

if __name__ == "__main__":
    met = theGood1()
    met()
    met()
    met2 = theGood1()
    met2()
    met2()
    met()