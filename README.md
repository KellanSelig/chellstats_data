#ChellStats - Data

Data side of [ChellStats!](https://chell-stats.herokuapp.com/)  webpage, which fetches, stores, and serves match data


[chellstats repository](https://github.com/migueog/chellstats)

##ETL
To run:
```bash
conda env update -f environment.yml
conda activate chellstats_data
cd etl
python3 main.py [-bq]
```
