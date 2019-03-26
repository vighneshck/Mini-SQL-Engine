import sys
import re
import csv

from crit_func import process

class start:
    def __init__(self):
        self.dict = {}

    def main(self):
        self.read_metadata()
        p = process(self.dict)
        p.process_query(str(sys.argv[1]))

    def read_metadata(self):
        f = open('./metadata.txt', 'r')
        flg = 0
        for i in f:
            if i.strip() == "<begin_table>":
                flg = 1
            elif flg == 1:
                table_name = i.strip()
                self.dict[table_name] = []
                flg = 0
            elif not i.strip() == '<end_table>':
                self.dict[table_name].append(i.strip())



s = start()
s.main()
