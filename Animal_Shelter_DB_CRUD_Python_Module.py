from pymongo import MongoClient, errors, ASCENDING, DESCENDING
from bson.objectid import ObjectId 

class Animal_Shelter(object): 
    """ CRUD operations for Animal collection in MongoDB """ 

    def __init__(self, user, password, host, port, database, collection, auth_source=None): 
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
        PORT = int(port) 
        DB = database 
        COL = collection 
        AUTH = auth_source or DB
        # 
        # Initialize Connection 
        # 
        self.client = MongoClient(f"mongodb://{USER}:{PASS}@{HOST}:{PORT}/{DB}?authSource={AUTH}")
        self.database = self.client['%s' % (DB)] 
        self.collection = self.database['%s' % (COL)]

        self.ensure_indexes()

    def ensure_indexes(self):
        """
        Create indexes that match the dashboard filter patterns.
        Safe to call repeatedly; MongoDB will ignore duplicates.
        """
        try:
            # Supports rescue filters (animal_type + sex + age range + breed membership)
            self.collection.create_index(
                [
                    ("animal_type", ASCENDING),
                    ("sex_upon_outcome", ASCENDING),
                    ("age_upon_outcome_in_weeks", ASCENDING),
                    ("breed", ASCENDING),
                ],
                name="idx_rescue_filters"
            )

            # Supports map lookups / general browsing patterns
            self.collection.create_index(
                [("location_lat", ASCENDING), ("location_long", ASCENDING)],
                name="idx_location"
            )
        except errors.PyMongoError as e:
            print(f"[WARN] Could not ensure indexes: {e}")

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

    def read(self, query, projection=None, limit=0, skip=0, sort=None):
        """
        Enhanced Read:
        - query: MongoDB filter dict
        - projection: dict of fields to include/exclude (e.g., {"_id": 0, "breed": 1})
        - limit: max docs (0 = no limit)
        - skip: offset for pagination
        - sort: list of tuples, e.g. [("breed", 1)] or [("age_upon_outcome_in_weeks", -1)]
        """
        if query is None:
            raise Exception("Query parameter is empty")

        try:
            cursor = self.collection.find(query, projection)

            if sort:
                # normalize "1/-1" sort inputs
                normalized = []
                for field, direction in sort:
                    normalized.append((field, ASCENDING if direction >= 0 else DESCENDING))
                cursor = cursor.sort(normalized)

            if skip and skip > 0:
                cursor = cursor.skip(int(skip))

            if limit and int(limit) > 0:
                cursor = cursor.limit(int(limit))

            return list(cursor)

        except errors.PyMongoError as e:
            print(f"An error occurred while reading the data: {e}")
            return []
            
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

    def breed_counts(self, query, limit=20):
        """
        Aggregation pipeline: returns top breeds and counts for the current filter.
        This avoids computing analytics client-side on the full table.
        """
        if query is None:
            query = {}

        pipeline = [
            {"$match": query},
            {"$group": {"_id": "$breed", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": int(limit)}
        ]

        try:
            results = list(self.collection.aggregate(pipeline))
            # normalize to friendly keys
            return [{"breed": r["_id"], "count": r["count"]} for r in results if r["_id"]]
        except errors.PyMongoError as e:
            print(f"[WARN] Aggregation failed: {e}")
            return []
    
    def count(self, query=None) -> int:
        """Return count of documents matching a query (used for pagination)."""
        query = query or {}
        try:
            return int(self.collection.count_documents(query))
        except errors.PyMongoError as e:
            print(f"[WARN] Count failed: {e}")
            return 0
