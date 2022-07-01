import mmap
import os
import random
from parallel import Parallel

from requests import head

class Inferring:
    
    def __init__(self, percent, seed= 0.01, header= True, sep=";", cores = 4):
        self.percent = percent
        self.seed = seed
        self.header = header
        self.sep = sep
        self.cores = cores
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


    def __process_record(record):
        pass
            

    def infer(self, filename):

        with open(filename, mode="r", encoding="utf8") as file_obj:
            with mmap.mmap(file_obj.fileno(), length=0, access=mmap.ACCESS_READ) as map:
                no_lines = self.__estimate_count(filename, map)
                portion = int(no_lines * self.percent)
                map.seek(0)

                self.__set_header(map.readline())

                random.seed(self.seed)

                lines = random.sample(map.read()
                                    .splitlines()[0:portion],
                                    portion)
                
                chunk_size = len(lines) // self.cores

                Parallel.parallel(lines, chunk_size, [self])
                
                Parallel.build_dataframe(lines)
                Parallel.infer_schema(self.dataframe)






    


    
