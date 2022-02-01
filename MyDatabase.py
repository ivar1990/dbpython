from asyncio.windows_events import NULL
from ctypes import sizeof
import os.path
import csv

class myDB:

    #default constructor
    def __init__(self):
        #csv file to be split into the 3 files
        #csv file
        self.filestream = None

        #delimiter
        self.delimiter = ","

        #name of the database
        self.DB_name = ""
        
        #3 files for the database
        #filenames for the 3 files

        #contains the fields of the records, total number of records, record size, db size,total number of overflow records 
        self.config_file = NULL
        #contains all the records
        self.data_file = NULL
        #TODO
        self.overflow_file = NULL

        #data contents of the 3 database files
        self.config_contents = NULL
        self.data_contents = NULL
        self.overflow_contents = NULL

        #total number of records in the data file
        self.num_record = 0

        #total number of records in the data file
        self.numSortedRecords = 0


        self.numOverflowRecords = 0

        #length of the longest record in the data file
        self.record_size = 0
        #size of the total number of record in the data file
        self.db_size = self.num_record * self.record_size

        self.isDBCreated = False
        self.isDBOpened = False

    def checkDBexists(self, filename):
        self.filestream = filename

        #name of the 3 files that will be created for this database
        self.config_file = self.filestream + "lf.config"
        self.data_file = self.filestream + "lf.data"
        self.overflow_file = self.filestream + "lf.overflow"

        #check if the database exists by checking if the folder
        #contains the 3 files(config, data, and overflow files) 
        if not os.path.isfile(self.config_file):
            print(str(self.filestream)+" not found")
            self.isDBCreated = False
            return self.isDBCreated

        if not os.path.isfile(self.data_file):
            print(str(self.filestream)+" not found")
            self.isDBCreated = False
            return self.isDBCreated

        if not os.path.isfile(self.overflow_file):
            print(str(self.filestream)+" not found")
            self.isDBCreated = False
            return self.isDBCreated

        #The 3 files for the database exists therefore the database was created
        self.isDBCreated = True
        return self.isDBCreated

    def createDB(self, filename):
        self.filestream = filename

        #name of the 3 files that will be created for this database
        self.config_file = self.filestream + "_lf.config"
        self.data_file = self.filestream + "_lf.data"
        self.overflow_file = self.filestream + "_lf.overflow"

        if self.checkDBexists(filename) == False :
            #create the files or if they exists then just open them
            self.config_contents = open(self.config_file, 'w+')
            self.data_contents = open(self.data_file, 'w+')
            self.overflow_contents = open(self.overflow_file, 'w+')

            #default values
            self.num_record = 0
            self.numSortedRecords = 0
            self.numOverflowRecords = 0


            #TODO
            #read the csv file 

            #increment the num_record variable
            #set the num_record to the numSortedRecords

            #write the csv data into the "_lf.data" file

            #close the database
            self.config_contents.close()
            self.data_contents.close()
            self.overflow_contents.close()
        

            self.isDBOpened = False
            self.isDBCreated = True


    def isCreated(self):
        return self.isDBCreated

    def open(self, filename): 
        if self.checkDBexists(filename) == False :  
            #open the database
            self.config_contents = open(self.config_file, 'w+')
            self.data_contents = open(self.data_file, 'w+')
            self.overflow_contents = open(self.overflow_file, 'w+')


            #set the variables from the config file
            self.num_record = 0
            self.numSortedRecords = 0
            self.numOverflowRecords = 0

            

            

            



            #indicate that the user can query if the database is opened
            self.isDBOpened = True
        else:
            self.isDBOpened = False

        return self.isDBOpened


    def isOpen(self):
        return self.isDBOpened


    def calculateRecordSize(self,filename):
        self.filestream = filename

        record_count = 0
        row_record_size = 0

        if not os.path.isfile(self.filestream):
            print(str(self.filename)+" not found")
            return 0
        else:
            csv_data = open(self.filestream, 'r+')
            #reading from file
            csv_reader = csv.reader(csv_data, delimiter=',')
            

            for row in csv_reader:
                
                row_record_size = sizeof(row)

                if row_record_size > self.record_size :
                    self.record_size = row_record_size

                record_count +=1
                print(f'Processed {record_count} lines.')

            self.num_record = self.numSortedRecords = record_count
            csv_data.close()
            
        return self.record_size


    def writeConfig(self)
        #writing to file
        config_writer = csv.writer(self.config_contents, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        #column names
        config_writer.writerow(['id', 'state', 'city', 'name'])
        #database stats
        config_writer.writerow([self.numSortedRecords, self.numOverflowRecords, self.record_size,self.db_size])    


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

        self.config_contents.close()
        self.data_contents.close()
        self.overflow_contents.close()

        self.num_record = 0
        self.numSortedRecords = 0
        self.numOverflowRecords = 0

        self.isDBOpened = False