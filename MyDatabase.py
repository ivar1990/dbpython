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

        #file position
        self.file_pos = 0

        #name of the database
        self.DB_name = ""
        
        #3 files for the database
        #filenames for the 3 files

        #contains the fields of the records, total number of records, record size, db size,total number of overflow records 
        self.config_file = ""
        #contains all the records
        self.data_file = ""
        self.overflow_file = ""

        #data contents of the 3 database files
        self.config_contents = None
        self.data_contents = None
        self.overflow_contents = None

        #total number of records in the data file
        self.num_record = 0

        #total number of records in the data file
        self.numSortedRecords = 0


        self.numOverflowRecords = 0

        #length of the longest record in the data file
        self.record_size = 0
        #size of the total number of record in the data file
        self.db_size = 0

        self.isDBCreated = False
        self.isDBOpened = False

    def checkDBexists(self, filename):
        self.filestream = filename

        #name of the 3 files that will be created for this database
        self.config_file = self.filestream + "_lf.config"
        self.data_file = self.filestream + "_lf.data"
        self.overflow_file = self.filestream + "_lf.overflow"

        #check if the database exists by checking if the folder
        #contains the 3 files(config, data, and overflow files) 
        if not os.path.isfile(self.config_file):
            print(str(self.config_file)+" not found")
            self.isDBCreated = False
            return self.isDBCreated

        if not os.path.isfile(self.data_file):
            print(str(self.data_file)+" not found")
            self.isDBCreated = False
            return self.isDBCreated

        if not os.path.isfile(self.overflow_file):
            print(str(self.overflow_file)+" not found")
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

            #read the csv file 
            #increment the num_record variable
            #set the num_record to the numSortedRecords
            self.calculateRecordSize(filename)

            #write the csv data into the "_lf.data" file
            self.writeConfig()

            #close the database
            self.config_contents.close()
            self.data_contents.close()
            self.overflow_contents.close()
        

            self.isDBOpened = False
            self.isDBCreated = True


    def isCreated(self):
        return self.isDBCreated

    def open(self, filename): 
        if self.checkDBexists(filename) == True :  
            #open the database
            self.config_contents = open(self.config_file, 'w+')
            self.data_contents = open(self.data_file, 'w+')
            self.overflow_contents = open(self.overflow_file, 'w+')


            #set the variables from the config file
            self.num_record = 0
            self.numSortedRecords = 0
            self.numOverflowRecords = 0

            self.readConfig()


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

        #check for the csv file
        if not os.path.isfile(filename):
            print(str(filename)+" not found")
            return 0
        else:
            csv_data = open(filename, 'r+')
            #reading from file
            csv_reader = csv.reader(csv_data, delimiter=',')
            

            for row in csv_reader:
                #get the size of the record
                row_record_size = len(row)
                print(f'row data {", ".join(row)}')

                #checks to see if the current record's size
                #is larger that the class's 
                if row_record_size > self.record_size :
                    self.record_size = row_record_size

                record_count +=1
                print(f'Processed {record_count} lines.')

            self.num_record = self.numSortedRecords = record_count
            self.db_size = self.num_record * self.record_size

            csv_data.close()
            
        return self.record_size


    def writeConfig(self):
        #writing to file
        config_writer = csv.writer(self.config_contents, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        #column names
        config_writer.writerow(['id', 'state', 'city', 'name'])
        #database stats
        config_writer.writerow([self.numSortedRecords, self.numOverflowRecords, self.record_size,self.db_size])   

    def readConfig(self):
        config_reader = csv.reader(self.config_contents,delimiter=',') 
        line_count = 0
        for row in config_reader:
            if line_count == 0:
                print(f'Column names are {", ".join(row)}')

            if line_count == 1:
                print(f'Database Stats are {", ".join(row)}')
                print(f'\t Number of Sorted Records {row[0]} Number of Overflow records {row[1]} Record Size {row[2]} DB Size {row[3]}.')
            
                self.numSortedRecords = row[0]
                self.numOverflowRecords = row[1]
                self.record_size = row[2]
                self.db_size = row[3]

            line_count += 1
        print(f'Processed {line_count} lines.')

    def writeRecord(self,file_pos,id,state,city,name):
        #writing to file
        data_writer = csv.writer(self.data_contents, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        #move to record position
        self.data_file.seek(0,0)
        self.data_file.seek((self.numSortedRecords-1) *self.record_size)

        #write data
        data_writer.writerow([id, state, city, name])
        
        return True

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

        #self.data.close()#part of the starter code i.e the data file contents

        self.config_contents.close()
        self.data_contents.close()
        self.overflow_contents.close()

        self.config_file = ""
        self.data_file = ""
        self.overflow_file = ""

        self.num_record = 0
        self.numSortedRecords = 0
        self.numOverflowRecords = 0

        #total number of records in the data file
        self.num_record = 0

        #total number of records in the data file
        self.numSortedRecords = 0


        self.numOverflowRecords = 0

        #length of the longest record in the data file
        self.record_size = 0
        #size of the total number of record in the data file
        self.db_size = 0

        self.isDBCreated = False
        self.isDBOpened = False