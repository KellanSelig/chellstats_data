#ChellStats - Data

Data side of [ChellStats!](https://chell-stats.herokuapp.com/) webpage. Includes an ETL to access
match data, and an API to serve match data & analysis to the ChellStats website.

ChellStats! website [GitHub](https://github.com/migueog/chellstats)

## Code Quality & Tests
Run `sh run_checks.sh`

##ETL
Because we are dealing with an undocumented API, our ETL options end up being a bit limited. 
As such, we currently only support fetching match data for the last ~50 matches at a time. We do this for all platforms,
and all teams in the top 100 on each platform.

Once we extract the data, we either write it to a local JSON file or load it into a BigQuery table
To run:
```bash
conda env update -f environment.yml --prune
conda activate chellstats_data
python -m etl [--method]
```
### Docker Commands
```bash
# build specific service container
docker-compose build [service]

# run service with optional --method arg
docker-compose run [service] [--method]

# stop service
docker-compose stop [service]

#remove stopped service containers
docker-compose rm [service]
```

### Deploy
To deploy the ETL container to cloud run and set up a pubsub, run `etl_deploy.sh`
