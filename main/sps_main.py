import sys,os
scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
superPath = os.path.realpath(os.path.dirname(scriptPath))
sys.path.append(superPath)
from ETLs.etl_spscommerce import SPScommerceETL
import json

if __name__ == "__main__":
    debug = False

    # storageServiceAccountCredential = json.load(open('./var/storage_service_account.json'))

    spsETL = SPScommerceETL({},debug=debug,areCredentialsLocal=True)
    # to ensure the code is working properly, 

    # history = spsETL.getTransactionsPaths(in_out="order")
    # print(history)
    # print(f"History: {len(history)}")

  
    print("Getting transactions paths")
    transactionsPaths = spsETL.getTransactionsPaths()
    print(f"Transactions paths: {len(transactionsPaths)} retrieved")

    print("Getting transactions paths")
    transactionsPaths = spsETL.getTransactionsPaths(in_out="out")
    print(f"Transactions paths: {len(transactionsPaths)} retrieved")

    # print("Getting transactions files")
    # files = spsETL.getTransactions(transactionsPaths)
    
    # print("Loading transactions to BigQuery")
    # spsETL.loadDataToBigQuery(files,'raw_SPScommerce_transactions_temp',force_schema=False)
    # print("Transactions loaded to BigQuery")