import itertools
import json
from pymongo import MongoClient

client = MongoClient()
my_db = client["my_mongo"]
posts = my_db.posts

keys_to_keep = ["id", "text", "smapp_timestamp", "user", "retweet_count", "retweeted", "retweeted_status"]

user_keys_to_keep = ["followers_count", "id_str", "protected", "friends_count", "statuses_count", "following", "statuses_count"]

with open("womens_march_2017_data__01_21_2017__00_00_00__23_59_59.json") as file:
    for line in file:
        try:
            j = json.loads(line)
            
            user = j["user"]
            
            unwanted_keys = set(j.keys()) - set(keys_to_keep)
            unwanted_user_keys = set(user.keys() - set(user_keys_to_keep))
        
            for unwanted_key in unwanted_keys:
                del j[unwanted_key]

            for unwanted_key in unwanted_user_keys:
                del user[unwanted_key]
                
            posts.insert_one(j)

        except Exception as e:
            print("Error with this JSON, {}".format(e))