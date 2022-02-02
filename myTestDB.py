from MyDatabase import myDB

filepath = "colleges-cr.csv"
DBsize = 67
rec_size = 71
# rec_size = 72 if a Windows file with cr lf at ends of the lines

sample = myDB()

sample.createDB("colleges-cr.csv")

if sample.isCreated():
    print("Database was Created.")
else:
    print("Database was not created")


sample.open("colleges-cr.csv")
if sample.isOpen():
    print("Database is open.")
else:
    print("Database was not opened")


#sample.readDB(filepath, DBsize, rec_size)

# print("\n------------- Testing getRecord ------------\n")
# sample.getRecord(-1)
# print("Record -1, ID: "+sample.record["ID"]+"\t experience: "+sample.record["experience"]+"\t marriage: "+sample.record["marriage"]+"\t wages: "+str(sample.record["wages"])+"\t industry: "+sample.record["industry"])
# sample.getRecord(0)
# print("Record 0, ID: "+sample.record["ID"]+"\t experience: "+sample.record["experience"]+"\t marriage: "+sample.record["marriage"]+"\t wages: "+str(sample.record["wages"])+"\t industry: "+sample.record["industry"])
# sample.getRecord(32)
# print("Record 32, ID: "+sample.record["ID"]+"\t experience: "+sample.record["experience"]+"\t marriage: "+sample.record["marriage"]+"\t wages: "+str(sample.record["wages"])+"\t industry: "+sample.record["industry"])
# sample.getRecord(66)
# print("Record 66, ID: "+sample.record["ID"]+"\t experience: "+sample.record["experience"]+"\t marriage: "+sample.record["marriage"]+"\t wages: "+str(sample.record["wages"])+"\t industry: "+sample.record["industry"])
# sample.getRecord(100)
# print("Record 100, ID: "+sample.record["ID"]+"\t experience: "+sample.record["experience"]+"\t marriage: "+sample.record["marriage"]+"\t wages: "+str(sample.record["wages"])+"\t industry: "+sample.record["industry"])

# print("\n------------- Testing binarySearch ------------\n")
# sample.binarySearch("00000")
# if sample.found:
#     print("ID: "+sample.record["ID"]+"\t experience: "+sample.record["experience"]+"\t marriage: "+sample.record["marriage"]+"\t wages: "+str(sample.record["wages"])+"\t industry: "+sample.record["industry"]+"\tRecord Number:" + str(sample.middle))
# else:
#     print("00000 not found, last position is: "+ str(sample.middle))

# sample.binarySearch("00067")
# if sample.found:
#     print("ID: "+sample.record["ID"]+"\t experience: "+sample.record["experience"]+"\t marriage: "+sample.record["marriage"]+"\t wages: "+str(sample.record["wages"])+"\t industry: "+sample.record["industry"]+"\tRecord Number:" + str(sample.middle))
# else:
#     print("00067 not found, last position is: "+ str(sample.middle))

# sample.binarySearch("00113")
# if sample.found:
#     print("ID: "+sample.record["ID"]+"\t experience: "+sample.record["experience"]+"\t marriage: "+sample.record["marriage"]+"\t wages: "+str(sample.record["wages"])+"\t industry: "+sample.record["industry"]+"\tLast position is:" + str(sample.middle))
# else:
#     print("00113 not found, last position is: "+ str(sample.middle))

# sample.binarySearch("10067")
# if sample.found:
#     print("ID: "+sample.record["ID"]+"\t experience: "+sample.record["experience"]+"\t marriage: "+sample.record["marriage"]+"\t wages: "+str(sample.record["wages"])+"\t industry: "+sample.record["industry"]+"\tLast position is:" + str(sample.middle))
# else:
#     print("10067 not found, last position is: "+ str(sample.middle))

sample.CloseDB()
