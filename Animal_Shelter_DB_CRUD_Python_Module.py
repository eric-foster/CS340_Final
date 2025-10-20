from pymongo import MongoClient, errors
from bson.objectid import ObjectId 

class Animal_Shelter(object): 
    """ CRUD operations for Animal collection in MongoDB """ 

    def __init__(self, user, password, host, port, database, collection): 
        # Initializing the MongoClient. This helps to access the MongoDB 
        # databases and collections. This is hard-wired to use the aac 
        # database, the animals collection, and the aac user. 
        # 
        # You must edit the password below for your environment. 
        # 
        # Connection Variables 
        # 
        USER = user
        PASS = password
        HOST = host 
        PORT = port 
        DB = database 
        COL = collection 
        # 
        # Initialize Connection 
        # 
        self.client = MongoClient('mongodb://%s:%s@%s:%d' % (USER,PASS,HOST,PORT)) 
        self.database = self.client['%s' % (DB)] 
        self.collection = self.database['%s' % (COL)] 
            
    # Create a method to return the next available record number for use in the create method
    def getNextRecordNum(self):
        # Query the animals collection for the document with the highest rec_num value
        out = self.database.animals.find().sort([('rec_num', -1)]).limit(1)
        
        # Loop through the result and return the next record number
        for dict in out:
            return (dict['rec_num'] + 1)
        
    # Create method to implement the C in CRUD. 
    def create(self, data):
        # Check that some data was provided
        if data is not None:
            try:
                # Insert the provided dictionary as a new document into the animals collection
                self.database.animals.insert_one(data)  # data should be dictionary
                return True # Indicate that the insert succeeded
            
            except errors.PyMongoError as e:
                # Catch and display any database related errors during insert
                print(f"An error occurred while inserting the data: {e}")
                return False # Indicate that the insert failed
            
        else: 
            # If no data is given, raise an exception rather than inserting nothing
            raise Exception("Nothing to save, because data parameter is empty") 

    # Create method to implement the R in CRUD.
    def read(self, query):
        # Ensure a query filter was provided
        if query is not None:
            try:
                # Find all documents that match the query filter
                # Convert the cursor to a list so it can be returned directly
                documents = list(self.collection.find(query))
                return documents # Return matching documents as a list
            
            except errors.PyMongoError as e:
                # Catch and display any database related errors during the query
                print(f"An error occurred while reading the data: {e}")
                return [] # Return an empty list if the read fails
        else:
            # If no query is given, raise an exception
            raise Exception("Query parameter is empty")
            
# Method to implement the U in CRUD.
    def update(self, lookup_pair, update_data):
        # Ensure both lookup filter and update data are provided
        if lookup_pair is not None and update_data is not None:
            try:
                # Check if update_data already contains an operator
                # If not, assume the user wants to set the fields
                if not any(key.startswith('$') for key in update_data.keys()):
                    update_operation = {'$set': update_data}
                else:
                    update_operation = update_data

                # Use update_many to allow for modification of multiple documents
                result = self.collection.update_many(lookup_pair, update_operation)
                
                # Return the count of documents modified
                return result.modified_count
            
            except errors.PyMongoError as e:
                # Catch and display any database related errors during the update
                print(f"An error occurred while updating the data: {e}")
                return 0 # Return 0 objects modified if an error occurs
            
        else:
            # Raise an exception if required parameters are missing
            raise Exception("Required parameters for update are missing: lookup_pair and/or update_data")

    # Method to implement the D in CRUD.
    def delete(self, lookup_pair):
        # Ensure a lookup filter was provided
        if lookup_pair is not None:
            try:
                # Use delete_many to allow for removal of multiple documents
                result = self.collection.delete_many(lookup_pair)
                
                # Return the count of documents removed
                return result.deleted_count
            
            except errors.PyMongoError as e:
                # Catch and display any database related errors during the delete
                print(f"An error occurred while deleting the data: {e}")
                return 0 # Return 0 objects removed if an error occurs
            
        else:
            # Raise an exception if the required parameter is missing
            raise Exception("Required parameter for delete is missing: lookup_pair")
        
            