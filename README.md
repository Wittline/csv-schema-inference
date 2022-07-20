# **Csv Schema Inference**
A tool to automatically infer columns data types in .csv files

### Check the article here:  <a href="https://itnext.io/building-a-schema-inference-data-pipeline-for-large-csv-files-7a45d41ad4df">Building a Schema Inference Data Pipeline for Large CSV files</a>

<p align="center">
  <img 
    src="https://user-images.githubusercontent.com/8701464/178112999-a80d984c-5dd7-44a6-bc83-a6eeaa2bf0c5.png"
  >
</p>


<div class="cell markdown" id="bDEfBKw0v5Gl">

## **Installing csv-schema-inference** 🔧

</div>

<div class="cell code" data-execution_count="5" data-colab="{&quot;base_uri&quot;:&quot;https://localhost:8080/&quot;}" id="NW7FOsRhtptl" data-outputId="2ad79008-9ec3-44e7-8e64-f990533c1fdc">

``` python
pip install csv-schema-inference
```

<div class="output stream stdout">

    Looking in indexes: https://pypi.org/simple, https://us-python.pkg.dev/colab-wheels/public/simple/
    Collecting csv-schema-inference
      Downloading csv_schema_inference-0.0.9-py3-none-any.whl (7.3 kB)
    Installing collected packages: csv-schema-inference
    Successfully installed csv-schema-inference-0.0.9

</div>

</div>

<div class="cell markdown" id="fciY6CMswOcV">

## **Importing csv-schema-inference library** ⚡

</div>

<div class="cell code" data-execution_count="6" id="ZCe2cOfJtxbB">

``` python
from csv_schema_inference import csv_schema_inference
```

</div>

<div class="cell markdown" id="ejVS9wb1wYK5">

## **Setting csv-schema-inference configuration** ✍

</div>

<div class="cell code" data-execution_count="7" id="MxqPQHl4t03W">

``` python

#if the inferred data type is INTEGER and there is a presence of FLOAT on the results , then the result will be FLOAT
conditions = {"INTEGER":"FLOAT"}

csv_infer = csv_schema_inference.CsvSchemaInference(portion=0.9, max_length=100, batch_size = 200000, acc = 0.8, seed=2, header=True, sep=",", conditions = conditions)
pathfile = "/content/file__500k.csv"
```

</div>

<div class="cell markdown" id="-DbG_LFKwvD0">

## **Run inference** 🏃

</div>

<div class="cell code" data-execution_count="8" id="Ta4HiDbDwuXO">

``` python
aprox_schema = csv_infer.run_inference(pathfile)
```

</div>

<div class="cell markdown" id="sN5Y5Uktwryp">

## **Showing the approximate data type inference for each column** 🔍

</div>

<div class="cell code" data-execution_count="9" data-colab="{&quot;base_uri&quot;:&quot;https://localhost:8080/&quot;}" id="lxUwb3hKwsKZ" data-outputId="d269d7d9-ea0b-490d-d83f-353b8548b179">

``` python
csv_infer.pretty(aprox_schema)
```

<div class="output stream stdout">

    0
    	name
    		id
    	type
    		INTEGER
    	nullable
    		False
    1
    	name
    		full_name
    	type
    		STRING
    	nullable
    		True
    2
    	name
    		age
    	type
    		INTEGER
    	nullable
    		False
    3
    	name
    		city
    	type
    		STRING
    	nullable
    		True
    4
    	name
    		weight
    	type
    		FLOAT
    	nullable
    		False
    5
    	name
    		height
    	type
    		FLOAT
    	nullable
    		False
    6
    	name
    		isActive
    	type
    		BOOLEAN
    	nullable
    		False
    7
    	name
    		col_int1
    	type
    		INTEGER
    	nullable
    		False
    8
    	name
    		col_int2
    	type
    		INTEGER
    	nullable
    		False
    9
    	name
    		col_int3
    	type
    		INTEGER
    	nullable
    		False
    10
    	name
    		col_float1
    	type
    		FLOAT
    	nullable
    		False
    11
    	name
    		col_float2
    	type
    		FLOAT
    	nullable
    		False
    12
    	name
    		col_float3
    	type
    		FLOAT
    	nullable
    		False
    13
    	name
    		col_float4
    	type
    		FLOAT
    	nullable
    		False
    14
    	name
    		col_float5
    	type
    		FLOAT
    	nullable
    		False
    15
    	name
    		col_float6
    	type
    		FLOAT
    	nullable
    		False
    16
    	name
    		col_float7
    	type
    		FLOAT
    	nullable
    		False
    17
    	name
    		col_float8
    	type
    		FLOAT
    	nullable
    		False
    18
    	name
    		col_float9
    	type
    		FLOAT
    	nullable
    		False
    19
    	name
    		col_float10
    	type
    		FLOAT
    	nullable
    		False
    20
    	name
    		test_column
    	type
    		FLOAT
    	nullable
    		False

</div>

</div>

<div class="cell markdown" id="LMP0nZNtxUvy">

## **Checking schema values for specific columns** ✔

</div>

<div class="cell code" data-execution_count="10" data-colab="{&quot;base_uri&quot;:&quot;https://localhost:8080/&quot;}" id="_fxgKtFDt3aH" data-outputId="0d09760a-a6b8-49f3-9230-61f8e61510d6">

``` python
result = csv_infer.get_schema_columns(columns = {"test_column"})
csv_infer.pretty(result)
```

<div class="output stream stdout">

    20
    	_name
    		test_column
    	types_found
    		INTEGER
    			cnt
    				406130
    		FLOAT
    			cnt
    				50964
    	nullable
    		False
    	type
    		FLOAT

</div>

</div>

<div class="cell markdown" id="tWIdQXTfx3hW">

## **Explore all possible data types for a specific columns** ✅

</div>

<div class="cell code" data-execution_count="11" data-colab="{&quot;base_uri&quot;:&quot;https://localhost:8080/&quot;}" id="d93OWWDMt5Qy" data-outputId="db73203d-9dcb-49de-dd00-8287ae9ca7d6">

``` python
result = csv_infer.explore_schema_column(column = "test_column")
csv_infer.pretty(result)
```

<div class="output stream stdout">

    20
    	name
    		test_column
    	types_found
    		INTEGER
    			88.85043339006856
    		FLOAT
    			11.149566609931437
    	nullable
    		False

</div>

</div>

### Benchmark
The tests were done with 9 .csv files, 21 columns, different sizes and number of records, an average of 5 executions was calculated for each process, shuffle time and inferring time.

- file__20m.csv: 20 million records
- file__15m.csv: 15 million records
- file__12m.csv: 12 million records
- file__10m.csv: 10 million records
- And so on...

If you want to know more about the shuffling process, you can check this other repository: "hhhh", the shuffling process helps us to:

1. Increase the probability of finding all the data types present in a single column. 
2. Avoid iterate the entire dataset.
2. Avoid see biases in the data that may be part of its organic behavior and due to not knowing the nature of its construction.

## Contributing and Feedback
Any ideas or feedback about this repository?. Help me to improve it.

## Authors
- Created by <a href="https://twitter.com/RamsesCoraspe"><strong>Ramses Alexander Coraspe Valdez</strong></a>
- Created on 2022

## License
This project is licensed under the terms of the MIT License.