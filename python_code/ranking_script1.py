

import pymongo
import json
import datetime

# Connecting to the database
def rankings():

    myclient=pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient['clear-dev']
    mycol = mydb['claims']

    # The ranks on providers and denials based on past
    with open('denial_rank.json','r') as read_file:
        denial_rank = json.load(read_file)
    with open('parnt_prov_rank.json','r') as read_file:
        prnt_prov_rank = json.load(read_file)

    # Ranking claims which have Process Flag - False
    cursor = mycol.find({'Process_Flag':False})
    current_date = datetime.datetime.now()

    for claim in cursor:
        rank = 0
        serv_date = claim['Service_Start_Date']
        exp = (current_date-serv_date).days

        for line in claim['Claim_Line']:
            amt = line['planInformation']['Plan_Billed_Amount']
            rank_denial_code = int(denial_rank[line['denialReason']['rsn_cd']])
            rank_prov = int(prnt_prov_rank[str(line['providerInformation']['Parent_Provider_ID'])])

            # Ranking formula
            rank += amt*rank_denial_code*rank_prov*0.01*exp
        rank = rank/claim['version']

        # Averaging rank for a claim based on its claim lines
        rank = rank/len(claim)

        # Updating the database with ranks
        mycol.update_one({'_id': claim['_id']}, {'$set': {'Rank': rank}}, upsert=False)
