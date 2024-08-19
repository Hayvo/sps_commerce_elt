from google.cloud import bigquery
import copy
import traceback
from lib.schema_generator import SchemaGenerator

class BigQueryLoader:
    def __init__(self,BQproject,debug = False):
        self.BQproject = BQproject
        self.SchemaGenerator = SchemaGenerator()
        self.debug = debug

    def removeDuplicatesFields(self,data):
        def recRemoveDuplicatesFields(json):
            if isinstance(json,dict):
                new_json = {}
                visited = {}
                for (field) in json:
                    if visited.get(str(field).lower(),False):
                        continue
                    else:
                        new_json[field.replace('-','_').replace('\r','').replace(' ','').replace('$','')] = recRemoveDuplicatesFields(json[field])
                        visited[str(field).lower()] = True
                return new_json
            elif isinstance(json,list):
                return [recRemoveDuplicatesFields(unit) for unit in json]
            elif isinstance(json,str):
                return json.replace('\r','')
            else:
                return json
        return recRemoveDuplicatesFields(data)
    
    def formatJSON(self, data):
        new_data = copy.deepcopy(data)
        def recFunc(dictio):
            if not isinstance(dictio,dict):
                return dictio
            for field in dictio:
                if field == 'version':
                    dictio[field] = int(eval(dictio[field]))
                if isinstance(dictio[field],dict):
                    if dictio[field] == {}:
                        dictio[field] = None
                    else:
                        dictio[field] = recFunc(dictio[field])
                if isinstance(dictio[field],list):
                    if dictio[field] == [{}]:
                        dictio[field] = None
                    elif len(dictio[field]) != 0:
                        if all(isinstance(unit,dict) for unit in dictio[field]):
                            dictio[field] = [recFunc(unit) for unit in dictio[field]]
                        else:
                            dictio[field] = dictio[field]
                    else:
                        dictio[field] = None
            return dictio
        for i,dictio in enumerate(new_data):
            new_data[i] = recFunc(dictio)
        return new_data

    def getSchema(self,new_data,client,dataset_ref,BQtable,base_table = None,force_schema = False):
        table_ref = dataset_ref.table(BQtable)
        if force_schema:
                print('Forcing schema...')
                schema = self.SchemaGenerator.generateSchema(new_data)
        else:   
            print('Getting schema...')
            if base_table is not None:
                try:
                    base_table_ref = dataset_ref.table(base_table)
                    client_base_table = client.get_table(base_table_ref)
                    print('Provided base table found, getting schema...')
                    schema = client_base_table.schema
                    return schema
                except Exception as e:      
                    print('Provided base table not found, generating schema...')
                    traceback.print_exc()
            else:
                if BQtable.endswith('_temp'):
                    new_BQtable = BQtable[:-5]
                    new_table_ref = dataset_ref.table(new_BQtable)
                    try:
                        client_new_table = client.get_table(new_table_ref)
                        print('Base table found, getting schema...')
                        schema = client_new_table.schema
                    except Exception as e:
                        print('No base table found, looking for temp table...')
                        try:
                            client_table = client.get_table(table_ref)
                            print('Temp table found, getting schema...')
                            schema = client_table.schema
                        except Exception as e:
                            schema = self.SchemaGenerator.generateSchema(new_data)
                            print('Temp table not found, generating schema...')
                else:
                    try:
                        client_table = client.get_table(table_ref)
                        print('Already a base table, getting schema...')
                        schema = client_table.schema
                    except Exception as e:
                        traceback.print_exc()
                        schema = self.SchemaGenerator.generateSchema(new_data)
                        print('Table not found, generating schema...')
                
        return schema
    
    def loadDataToBQ(self,data,BQdataset,BQtable,base_table = None,force_schema = False,WRITE_DISPOSITION='WRITE_TRUNCATE'):
        client = bigquery.Client(project=self.BQproject)
        dataset_ref = client.dataset(BQdataset, project = self.BQproject)
        table_ref = dataset_ref.table(BQtable)  
        try:
            dataset = client.get_dataset(dataset_ref)
            print('Dataset found')
        except Exception as e:
            print('Dataset not found, creating dataset...')
            dataset = bigquery.Dataset(dataset_ref)
            dataset = client.create_dataset(dataset)
            print('Dataset created')
        print('Removing duplicates fields and formatting JSON...')
        new_data = self.removeDuplicatesFields(self.formatJSON(data))
        schema = self.getSchema(new_data,client,dataset_ref,BQtable,base_table,force_schema)
        if not(self.debug):
            try:
                print('Loading data to BigQuery...')
                job_config = bigquery.job.LoadJobConfig(schema = schema, autodetect = False)
                if WRITE_DISPOSITION == "WRITE_TRUNCATE":
                    job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE
                elif WRITE_DISPOSITION == 'WRITE_APPEND':
                    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
                job_config.create_disposition = bigquery.job.CreateDisposition.CREATE_IF_NEEDED
                job = client.load_table_from_json(new_data,table_ref,job_config = job_config,num_retries=10)
                print(job.result()) 
            except Exception as e:
                print('Error loading data to BigQuery, trying to force schema...')
                schema = self.SchemaGenerator.generateSchema(new_data,schema)
                if WRITE_DISPOSITION == "WRITE_APPEND":
                    print("Updating schema of temp table...")
                    table_ref = dataset_ref.table(BQtable)
                    client_table = client.get_table(table_ref)
                    client_table.schema = schema
                    client.update_table(client_table,['schema'])
                    print('Schema updated')
                job_config = bigquery.job.LoadJobConfig(schema = schema, autodetect = False)
                if WRITE_DISPOSITION == "WRITE_TRUNCATE":
                    job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE
                elif WRITE_DISPOSITION == 'WRITE_APPEND':
                    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
                job_config.create_disposition = bigquery.job.CreateDisposition.CREATE_IF_NEEDED
                job = client.load_table_from_json(new_data,table_ref,job_config = job_config,num_retries=10)
                print(job.result()) 
                try:
                    print('Updating schema of base table...')
                    if base_table is not None:
                        base_table_ref = dataset_ref.table(base_table)
                        client_base_table = client.get_table(base_table_ref)
                        client_base_table.schema = schema
                        try:
                            client.update_table(client_base_table,['schema'])
                            print('Schema updated')
                        except Exception as e:
                            print('Error updating schema of base table')
                            traceback.print_exc
                    elif BQtable.endswith('_temp'):
                            new_BQtable = BQtable[:-5]
                            new_table_ref = dataset_ref.table(new_BQtable)
                            try:
                                client_new_table = client.get_table(new_table_ref)
                                client_new_table.schema = schema
                                client.update_table(client_new_table,['schema'])
                                print('Schema updated')
                            except Exception as e:
                                print('Error updating schema of base table')
                                traceback.print_exc
                except Exception as e:
                    print('No base table found')
                    traceback.print_exc
        else:
            print('Debug mode, not loading data to BigQuery')
            schema = self.SchemaGenerator.generateSchema(new_data,schema)
            print(schema)

