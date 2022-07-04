import mmap
import os
import random
import multiprocessing as mp


class DetectType:

    def process(records):
        pass

class Parallel:

    def __init__(self):
        pass

    
    def execute(self, s, i):

        if self.isencode:
            for i in range(0, len(self.obj)):
                s = self.obj[i].encode(s)
        else:
            for i in range(0, len(self.obj)):
                s = self.obj[i].decode(s)
    
        return s

        
    def parallel(self, records, obj,  chunk_size = None):
        
        cpus = (mp.cpu_count() - 1)
        if chunk_size is None:
            chunk_size = len(records) // cpus                

        pool = mp.Pool(processes=cpus)
        
        results = [pool.apply_async(self.execute, args=(records[x:x+self.chunk_size], x, obj)) for x in range(0, len(records), self.chunk_size)]
        pool.close()
        pool.join()
        
        outputs = [p.get() for p in results]

        output = []

        for _out in outputs:
            output += _out

        return output

    

class CsvSchemaInference:
    
    def __init__(self, percent, seed= 0.01, header= True, sep=";"):
        self.percent = percent
        self.seed = seed
        self.header = header
        self.sep = sep
        self.dataframe = {}

        

    def __set_header(self, header):

        header = header.rstrip().split(self.sep)

        for i in range(0, len(header)):
            self.dataframe[i] = {
                "column_name": header[i],
                "type":"STRING",
                "nullable":False,
                "values" : []
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

                types = Parallel.parallel(records, [DetectType], chunk_size = None)









    


    
