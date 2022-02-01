from asyncio.windows_events import NULL
import os.path
import csv
from pickle import NONE

class DB:

    #default constructor
    def __init__(self):
        self.filestream = NONE
        self.num_record = 0

###################################################################################from the starter code

    #read the database
    def readDB(self, filename, DBsize, rec_size):
        self.filestream = filename
        self.record_size = DBsize
        self.rec_size = rec_size
        
        if not os.path.isfile(self.filestream):
            print(str(self.filestream)+" not found")
        else:
            self.data = open(self.filestream, 'r+')

    #read record method
    def getRecord(self, recordNum):

        self.flag = False
        id = experience = marriage = wage = industry = "None"

        if recordNum >=0 and recordNum < self.record_size:
            self.data.seek(0,0)
            self.data.seek(recordNum*self.rec_size)
            line= self.data.readline().rstrip('\n')
            self.flag = True
        
        if self.flag:
            id, experience, marriage, wage, industry = line.split()

        self.record = dict({"ID":id,"experience":experience,"marriage":marriage,"wages":wage,"industry":industry})

    #Binary Search by record id
    def binarySearch (self, input_ID):
        
        low = 0
        high = self.record_size - 1
        self.found = False

        while high >= low:

            self.middle = (low+high)//2
            self.getRecord(self.middle)
            # print(self.record)
            mid_id = self.record["ID"]
            
            if int(mid_id) == int(input_ID):
                self.found = True
                break
            elif int(mid_id) > int(input_ID):
                high = self.middle - 1
            elif int(mid_id) < int(input_ID):
                low = self.middle + 1
################################################################################from the starter code

    #close the database
    def CloseDB(self):

        self.data.close()#part of the starter code i.e the data file contents

