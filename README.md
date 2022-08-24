# PolyFrame: A DataFrame interface for database systems

Python data exploration library that integrates a Pandas-like user experience with various database systems to provide analysts with a familiar environment while scaling out the analytical operations over a large data cluster for Big Data analysis.

**Note:** This library serves as a proof-of-concept code accompanying our research [paper](https://dl.acm.org/doi/abs/10.14778/3476249.3476281). Not all dataframe functions are/can be implemented and fully tested. Proceed with caution.

### Dependencies
* Python >= 3.3
* [Pip](https://pip.pypa.io/en/stable/)
* Java >= 1.8

### Installation
* Clone the repository
* Install from source (pip install . )

### Targeted Database Systems
* [AsterixDB](https://github.com/apache/asterixdb/) 
* [MongoDB](https://www.mongodb.com/) (Community Edition)
* [Neo4j](https://neo4j.com/) 
* [PostgreSQL](https://www.postgresql.org/) 
* [InfluxDB](https://www.influxdata.com/) (In progress)

Example usages of PolyFrame can be found under 'notebooks' folder.

### Example Notebooks
* Scale-independent Data Analysis with Database-backed Dataframes: a Case Study. EDBT/ICDT Workshops 2021 <br /> [[notebook]](https://github.com/psinthong/SF_CRIME_Notebook) [[paper]](http://ceur-ws.org/Vol-2841/SIMPLIFY_3.pdf)
* Exploratory Data Analysis with Database-backed Dataframes: A Case Study on Airbnb Data. IEEE BigData 2021: 3119-3129 <br /> [[notebook]](https://github.com/psinthong/PolyFrame_Case_Study) [[paper]](https://ieeexplore.ieee.org/document/9671603)


### Related Publications
* PolyFrame: A Retargetable Query-based Approach to Scaling Dataframes. Proc. VLDB Endow. 14(11): 2296-2304 (2021) <br /> [[paper]](http://www.vldb.org/pvldb/vol14/p2296-sinthong.pdf)
* AFrame: Extending DataFrames for Large-Scale Modern Data Analysis. IEEE BigData 2019: 359-371 <br /> [[paper]](https://ieeexplore.ieee.org/document/9006303)
