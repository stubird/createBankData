import json
import os,sys

def findplay():
    print("gogo")

sys.path.append('../')

from fullDataCreator import test


def theGood1():
    print("wawawa12 aaaa")

class aaa(object):
    @classmethod
    def theGood(self):
        print("wawawa aaaa")

if __name__ == "__main__":
    getattr(test,"theGood1")()

    af = compile("a = 12;b=234","<string>","exec")
    exec(af)
    print(a,b)