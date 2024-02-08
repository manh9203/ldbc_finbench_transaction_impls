# Kuzu FinBench implementation

A Kuzu reference implementation for the [LDBC Financial Benchmark](https://ldbcouncil.org/benchmarks/finbench/).

## 1. Preparation
- Have the dataset downloaded from [this drive](https://drive.google.com/drive/folders/1NIAo4KptskBytbXoOqmF3Sto4hTX3JIH)
- Download the latest version of Kuzu Java API [here](https://github.com/kuzudb/kuzu/releases/tag/v0.2.0). After the jar file is downloaded and placed into the project directory, it can be referenced in classpath manually with the -cp option.
- Reside the jar file and the dataset in following directory structure
```
|-- src/main
|   |-- java/...
|   |-- resources
|   |   |-- kuzu_java.jar
|   |   |-- data
|   |   |   |-- incremental
|   |   |   |-- read_params
|   |   |   |-- snapshot
|   |   |-- .gitignore
```

## 2. Loading data
Run 
```
cd kuzu 
python3 scripts/import_data.py 
```

## 3. Benchmark
Run for SF = 1:
```
java -cp target/kuzu-0.1.0-alpha.jar:target/classes/kuzu_java.jar org.ldbcouncil.finbench.driver.driver.Driver -P sf1_finbench_benchmark.properties   
```

Run for SF = 10:
```
java -cp target/kuzu-0.1.0-alpha.jar:target/classes/kuzu_java.jar org.ldbcouncil.finbench.driver.driver.Driver -P sf10_finbench_benchmark.properties  
```