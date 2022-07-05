import mmap
import os
import random
import multiprocessing as mp
import datetime as dt



class DetectType:

    def __init__(self, max_length, sep):
        self.max_length = max_length
        self.sep = sep
    
    def __get_local_type(value):
        try:
            float(value)
        except ValueError:
            return "STRING"
        
        if float(value).is_integer():
            return "INTEGER"
        else:        
            return "FLOAT"


    def __get_date_type(self, value):


        if "T" in value:
            segments = value.split("T")            
            try:
                
                if len(segments) == 2:
                    valid_date = False
                    d_elements = segments[0].split("-")
                    if len(d_elements) == 3 and len(d_elements[0]) in {2, 4} and \
                            len(d_elements[1]) == 2 and len(d_elements[2]) == 2:
                        dt.date(*(int(e) for e in d_elements))
                        valid_date = True
                    t_elements = segments[1].split(":")
                    valid_time = False
                    if len(t_elements) in (2, 3):
                        valid_time = (len(t_elements[0]) == 2 and 0 <= int(t_elements[0]) < 24 and
                                    len(t_elements[1]) and 0 <= int(t_elements[1]) < 60)
                        if len(t_elements) == 3:
                            valid_time = (valid_time and len(t_elements[2]) == 2 and
                                        0 <= int(t_elements[2]) < 60)
                    if valid_time and valid_date:
                        return "TIMESTAMP"

            except ValueError:
                return "STRING"

        elif "-" in value:

            segments = value.split("-")
            try:
                
                if len(segments) == 3 and len(segments[0]) in {2, 4} and \
                    len(segments[1]) == 2 and len(segments[2]) == 2:
                
                    dt.date(*(int(e) for e in segments))
                    return "DATE"
            except ValueError:
                return "STRING"
        else:

            try:
                segments = value.split(":")
                if len(segments) in {2, 3}:
                    valid = (len(segments[0]) == 2 and 0 <= int(segments[0]) < 24 and
                            len(segments[1]) and 0 <= int(segments[1]) < 60)
                    if len(segments) == 3:
                        valid = (valid and len(segments[2]) == 2 and
                                0 <= int(segments[2]) < 60)
                    if valid:
                        return "TIME"
            except ValueError:
                return "STRING"


        return "STRING"


    def __infer_value_type(self, value, index, schema):        
        
        if value not in schema[index]["values"].keys():
            
            local_type = self.__get_local_type(value)
            
            if local_type == 'STRING':

                value = value[0:100]
                
                if value in {"", 'na', 'NA', 'null', 'NULL'}:
                    schema[index]["nullable"] = True
                    _type = 'NULL'
                elif value in {"true", "false", "TRUE", "FALSE", "True", "False"}:
                    _type = 'BOOLEAN'                        
                elif len(value) < 21:
                    _type = self.__get_date_type(value)
                else:
                    _type = local_type
            else:
                _type = local_type
            
            schema[index]["values"][value] = { "count": 1,"date_type": _type}
        
        else:
            schema[index]["values"][value]["count"] += 1
            

    def execute(self, records, schema):

        for record in records:
            values = record.rstrip().split(self.sep)
            for index, value in enumerate(values):
                self.__infer_value_type(value[0:self.max_length], index, schema)
        
        return schema


class Parallel:

    def __init__(self):
        pass

    
    def execute(self, records, x, arr_obj, d_schema):

        for i in range(0, len(arr_obj)):
            d_schema = arr_obj[i].execute(records, d_schema)
        
        return d_schema


        
    def parallel(self, records, arr_obj,  d_schema, chunk_size = None):
        
        cpus = (mp.cpu_count() - 1)
        if chunk_size is None:
            chunk_size = len(records) // cpus                

        pool = mp.Pool(processes=cpus)
        
        results = [pool.apply_async(self.execute, args=(records[x:x+chunk_size], x, arr_obj, d_schema)) for x in range(0, len(records), chunk_size)]
        pool.close()
        pool.join()

        outputs = [p.get() for p in results]

        return outputs
        


class CsvSchemaInference:
    
    def __init__(self, percent, max_length = 100, column_accuracy = 0.8, seed= 0.01, header= True, sep=";"):
        self.percent = percent
        self.seed = seed
        self.header = header
        self.sep = sep
        self.schema = {}
        self.column_accuracy = column_accuracy
        self.max_length = max_length        
        
  

    def __set_header(self, header):
        
        header = str(header).rstrip().split(self.sep)
        for i in range(0, len(header)):
            self.schema[i] = {
                "column_name": header[i],
                "values":{
                },
                "nullable":False         
            }


    def __estimate_count(self, filename, reader):
        buffer = reader.read(1<<13)
        file_size = os.path.getsize(filename)
        return file_size // (len(buffer) // buffer.count(b'\n'))
            

    def infer(self, filename):

        with open(filename, mode="r", encoding="utf8") as file_obj:

            with mmap.mmap(file_obj.fileno(), length=0, access=mmap.ACCESS_READ) as map:
                
                no_lines = self.__estimate_count(filename, map)
                portion = int(no_lines * self.percent)
                map.seek(0)

                self.__set_header(map.readline())

                random.seed(self.seed)

                records = random.sample(map.read()
                                    .splitlines()[0:portion],
                                    portion)
                                    
                prl = Parallel()
                dtype = DetectType(self.max_length, self.sep)

                schemas = prl.parallel(records = records, arr_obj=[dtype], d_schema = self.schema, chunk_size = None)

                for schema in schemas:
                    print(schema)

 