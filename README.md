# **Csv Schema Inference**
A tool to automatically infer columns data types in .csv files

### Check the article here:  <a href="https://itnext.io/building-a-schema-inference-data-pipeline-for-large-csv-files-7a45d41ad4df">Building a Schema Inference Data Pipeline for Large CSV filesn</a>

<p align="center">
  <img 
    src="https://user-images.githubusercontent.com/8701464/177665684-49ef381a-cd67-4290-aea1-cd62f315a005.png"
  >
</p>


<div class="cell markdown" id="bDEfBKw0v5Gl">

## **Installing csv-schema-inference** 🔧

</div>

<div class="cell code" data-execution_count="1" data-colab="{&quot;base_uri&quot;:&quot;https://localhost:8080/&quot;}" id="NW7FOsRhtptl" data-outputId="686e0438-b703-441d-a956-efa8c5876570">

``` python
pip install csv-schema-inference
```

<div class="output stream stdout">

    Looking in indexes: https://pypi.org/simple, https://us-python.pkg.dev/colab-wheels/public/simple/
    Collecting csv-schema-inference
      Downloading csv_schema_inference-0.0.3-py3-none-any.whl (5.2 kB)
    Installing collected packages: csv-schema-inference
    Successfully installed csv-schema-inference-0.0.3

</div>

</div>

<div class="cell markdown" id="fciY6CMswOcV">

## **Importing csv-schema-inference library** ⚡

</div>

<div class="cell code" data-execution_count="2" id="ZCe2cOfJtxbB">

``` python
from csv_schema_inference import csv_schema_inference
```

</div>

<div class="cell markdown" id="ejVS9wb1wYK5">

## **Setting csv-schema-inference configuration** ✍

</div>

<div class="cell code" data-execution_count="3" id="MxqPQHl4t03W">

``` python
csv_infer = csv_schema_inference.CsvSchemaInference(portion=0.7, max_length=100, seed=2, header=True, sep=",")
pathfile = "/content/data.csv"
```

</div>

<div class="cell markdown" id="-DbG_LFKwvD0">

## **Run inference** 🏃

</div>

<div class="cell code" data-execution_count="4" id="Ta4HiDbDwuXO">

``` python
aprox_schema = csv_infer.run_inference(pathfile)
```

</div>

<div class="cell markdown" id="sN5Y5Uktwryp">

## **Showing the approximate data type inference for each column** 🔍

</div>

<div class="cell code" data-execution_count="5" data-colab="{&quot;base_uri&quot;:&quot;https://localhost:8080/&quot;}" id="lxUwb3hKwsKZ" data-outputId="bdfb0213-456e-46b4-c39c-d45bb24dfff6">

``` python
csv_infer.pretty(aprox_schema)
```

<div class="output stream stdout">

    0
    	name
    		key_1
    	type
    		STRING
    	nullable
    		False
    1
    	name
    		date_2
    	type
    		DATE
    	nullable
    		False
    2
    	name
    		cont_3
    	type
    		FLOAT
    	nullable
    		False
    3
    	name
    		cont_4
    	type
    		FLOAT
    	nullable
    		False
    4
    	name
    		disc_5
    	type
    		INTEGER
    	nullable
    		False
    5
    	name
    		disc_6
    	type
    		INTEGER
    	nullable
    		True
    6
    	name
    		cat_7
    	type
    		STRING
    	nullable
    		False
    7
    	name
    		cat_8
    	type
    		STRING
    	nullable
    		False
    8
    	name
    		cont_9
    	type
    		FLOAT
    	nullable
    		False
    9
    	name
    		cont_10
    	type
    		FLOAT
    	nullable
    		True

</div>

</div>

<div class="cell markdown" id="LMP0nZNtxUvy">

## **Checking schema values for specific columns** ✔

</div>

<div class="cell code" data-execution_count="12" data-colab="{&quot;base_uri&quot;:&quot;https://localhost:8080/&quot;}" id="_fxgKtFDt3aH" data-outputId="2f78136f-1351-41c0-bf9a-9f700371dd2d">

``` python
result = csv_infer.get_schema_columns(columns = {"disc_6"})
csv_infer.pretty(result)
```

<div class="output stream stdout">

    5
    	_name
    		disc_6
    	values
    		na
    			cnt
    				70755
    			_type
    				STRING
    		14
    			cnt
    				34732
    			_type
    				INTEGER
    		17
    			cnt
    				35237
    			_type
    				INTEGER
    		12
    			cnt
    				35408
    			_type
    				INTEGER
    		10
    			cnt
    				35174
    			_type
    				INTEGER
    		4
    			cnt
    				34924
    			_type
    				INTEGER
    		8
    			cnt
    				34861
    			_type
    				INTEGER
    		7
    			cnt
    				35270
    			_type
    				INTEGER
    		13
    			cnt
    				35274
    			_type
    				INTEGER
    		5
    			cnt
    				35024
    			_type
    				INTEGER
    		0
    			cnt
    				35325
    			_type
    				INTEGER
    		2
    			cnt
    				35265
    			_type
    				INTEGER
    		16
    			cnt
    				35250
    			_type
    				INTEGER
    		6
    			cnt
    				34961
    			_type
    				INTEGER
    		15
    			cnt
    				35132
    			_type
    				INTEGER
    		11
    			cnt
    				35250
    			_type
    				INTEGER
    		3
    			cnt
    				35063
    			_type
    				INTEGER
    		1
    			cnt
    				35237
    			_type
    				INTEGER
    		9
    			cnt
    				35078
    			_type
    				INTEGER
    	nullable
    		True
    	approximate_type
    		INTEGER

</div>

</div>

<div class="cell markdown" id="tWIdQXTfx3hW">

## **Explore all possible data types for a specific columns** ✅

</div>

<div class="cell code" data-execution_count="13" data-colab="{&quot;base_uri&quot;:&quot;https://localhost:8080/&quot;}" id="d93OWWDMt5Qy" data-outputId="6b7e8aed-bbbe-4f5e-f2ea-d1d3e8d4606f">

``` python
result = csv_infer.explore_schema_column(column = "disc_6")
csv_infer.pretty(result)
```

<div class="output stream stdout">

    5
    	name
    		disc_6
    	types
    		STRING
    			10.061573902903785
    		INTEGER
    			89.93842609709621
    	nullable
    		True

</div>

</div>

## Contributing and Feedback
Any ideas or feedback about this repository?. Help me to improve it.

## Authors
- Created by <a href="https://twitter.com/RamsesCoraspe"><strong>Ramses Alexander Coraspe Valdez</strong></a>
- Created on 2022

## License
This project is licensed under the terms of the MIT License.
