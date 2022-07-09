import mmap
import os
import random
import multiprocessing as mp
import datetime as dt
import operator



class DetectType:

    def __init__(self, max_length, sep):
        self.max_length = max_length
        self.sep = sep
    
    def __get_local_type(self, value):
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
                
                if value in {"", "na", "NA", "null", "NULL"}:
                    schema[index]["nullable"] = True
                    _type = "STRING"
                elif value in {"true", "false", "TRUE", "FALSE", "True", "False"}:
                    _type = "BOOLEAN"                
                elif len(value) < 21:
                    _type = self.__get_date_type(value)
                else:
                    _type = local_type
            else:
                _type = local_type
            
            schema[index]["values"][value] = { "cnt": 1,"_type": _type}
        
        else:
            schema[index]["values"][value]["cnt"] += 1
            

    def execute(self, records, schema):

        for record in records:
            values = record.rstrip().split(self.sep)            
            for index, value in enumerate(values):
                self.__infer_value_type(value[0:self.max_length], index, schema)
        
        return schema


class Parallel:

    def __init__(self):
        pass

    
    def execute(self, records, x, obj, d_schema):
        return obj.execute(records, d_schema)        


        
    def parallel(self, records, obj,  d_schema, chunk_size = None):
        
        cpus = (mp.cpu_count() - 1)
        if chunk_size is None:
            chunk_size = len(records) // cpus                

        pool = mp.Pool(processes=cpus)
        
        results = [pool.apply_async(self.execute, args=(records[x:x+chunk_size], x, obj, d_schema)) for x in range(0, len(records), chunk_size)]
        pool.close()
        pool.join()


        outputs = [p.get() for p in results]

        return outputs
        


class CsvSchemaInference:
    
    def __init__(self, portion = 0.5, max_length = 100, acc = 0.7, seed= 1, header= True, sep=";"):
        self.portion = portion
        self.seed = seed
        self.header = header
        self.sep = sep
        self.accuracy = acc
        self.__schema = {}
        self.max_length = max_length        
        
  

    def __set_header(self, header):
                
        header = header.rstrip().split(self.sep)
        for i in range(0, len(header)):
            self.__schema[i] = {
                "_name": header[i].replace('"', ''),
                "values":{
                },
                "nullable":False,
                "approximate_type":""
            }


    def __estimate_count(self, filename, reader):
        buffer = reader.read(1<<13)
        file_size = os.path.getsize(filename)
        return file_size // (len(buffer) // buffer.count(b'\n'))

    
    def __build_schema(self, schemas):

        for c_inx in self.__schema:

            for s_inx in range(0, len(schemas)):

                v_dict = schemas[s_inx][c_inx]

                if v_dict['nullable']:
                    self.__schema[c_inx]['nullable'] = True

                for k in v_dict['values']:

                    if k not in self.__schema[c_inx]['values']:

                        self.__schema[c_inx]['values'][k] = { 
                                    "cnt": v_dict['values'][k]['cnt'],
                                    "_type": v_dict['values'][k]['_type']
                                    }
                    else:
                        self.__schema[c_inx]['values'][k]['cnt'] += v_dict['values'][k]['cnt']
    


    def __approximate_types(self, acc = 0.5):

        result = {}        
        for c in self.__schema:
            _types = {}
            t = 0
            for v in self.__schema[c]['values']:
                value = self.__schema[c]['values'][v]
                t += value['cnt']
                if value['_type'] not in _types:
                    _types[value['_type']] = value['cnt']
                else:
                    _types[value['_type']] += value['cnt']

            for ft in _types:
                p = (_types[ft] * 100) / t
                _types[ft] = p
            

            try:
                _type = max({k: v for k, v in _types.items() if v >= (acc * 100)}.items(), 
                            key=operator.itemgetter(1))[0]
            except ValueError:
                _type = "STRING"


            self.__schema[c]['approximate_type'] = _type

            result[c] = {
                "name": self.__schema[c]['_name'], 
                "type": _type,
                "nullable": self.__schema[c]['nullable']
                }
        
        return result

        
    def pretty(self, d, ind=0):

        for k, v in d.items():
            print('\t' * ind + str(k))
            if isinstance(v, dict):
                self.pretty(v, ind+1)
            else:
                print('\t' * (ind+1) + str(v))


    def get_schema_columns(self, columns = {}):


        result = {}

        for c in self.__schema:
            if self.__schema[c]["_name"] in columns:
                result[c] = {
                    "_name": self.__schema[c]["_name"],
                    "values":self.__schema[c]["values"],
                    "nullable":self.__schema[c]["nullable"],
                    "approximate_type":self.__schema[c]["approximate_type"]
                }
        
        return result

    
    def explore_schema_column(self, column):
        
        result = {}
        
        for c in self.__schema:

            if column == self.__schema[c]['_name']:

                _types = {}
                t = 0
                for v in self.__schema[c]['values']:
                    value = self.__schema[c]['values'][v]
                    t += value['cnt']
                    if value['_type'] not in _types:
                        _types[value['_type']] = value['cnt']
                    else:
                        _types[value['_type']] += value['cnt']

                for ft in _types:
                    p = (_types[ft] * 100) / t
                    _types[ft] = p

                _types
                result[c] = {
                    "name" : self.__schema[c]['_name'],
                    "types": _types,
                    "nullable": self.__schema[c]['nullable']
                }
                
                break
        
        return result



    def run_inference(self, filename):

        with open(filename, mode="r", encoding="utf8") as file_obj:

            with mmap.mmap(file_obj.fileno(), length=0, access=mmap.ACCESS_READ) as map:
                
                no_lines = self.__estimate_count(filename, map)
                portion = int(no_lines * self.portion)
                map.seek(0)

                if self.header:
                    self.__set_header(map.readline().decode("utf-8"))                

                random.seed(self.seed)

                records = random.sample(map.read()
                                    .decode("utf-8")
                                    .splitlines(),
                                    portion)                            
                                    
                prl = Parallel()
                dtype = DetectType(self.max_length, 
                                   self.sep)

                schemas = prl.parallel(records = records,
                                      obj=dtype, 
                                      d_schema = self.__schema, 
                                      chunk_size = None)

                #Joining schemas results
                self.__build_schema(schemas)           

                #Approximate data types
                return self.__approximate_types(acc = self.accuracy)