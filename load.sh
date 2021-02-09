#!/bin/bash

# Load short-term mortality data for the last few years.
echo stmf.csv
bq load \
    --project_id=fivetran-wild-west \
    --source_format=CSV \
    --skip_leading_rows=3 \
    --schema=country_code:string,year:int64,week:int64,sex:string,d0_14:float64,d15_64:float64,d65_74:float64,d75_84:float64,d85p:float64,dtotal:float64,r0_14:float64,r15_64:float64,r65_74:float64,r75_84:float64,r85p:float64,rtotal:float64,split:bool,splitsex:bool,forecast:bool \
    --replace \
    mortality.stmf \
    data/stmf.csv

# Load population data.
bq rm \
    --project_id=fivetran-wild-west \
    --force=true \
    mortality.population 
bq mk \
    --project_id=fivetran-wild-west \
    --schema=pop_name:string,area:string,sex:string,age:string,age_interval:string,type:string,day:int64,month:int64,year:int64,ref_code:int64,access:string,population:float64,note_code_1:string,note_code_2:string,note_code_3:string,ldb:string \
    mortality.population 
for f in data/*pop.txt; do
    echo $f
    bq load \
        --project_id=fivetran-wild-west \
        --source_format=CSV \
        --skip_leading_rows=1 \
        --null_marker=. \
        mortality.population \
        $f
done

# Load deaths data
bq rm \
    --project_id=fivetran-wild-west \
    --force=true \
    mortality.deaths 
bq mk \
    --project_id=fivetran-wild-west \
    --schema=pop_name:string,area:string,year:int64,year_reg:int64,year_interval:int64,sex:string,age:string,age_interval:string,lexis:string,refcode:int64,access:string,deaths:float64,note_code_1:string,note_code_2:string,note_code_3:string,ldb:string \
    mortality.deaths 
for f in data/*death.txt; do
    echo $f
    bq load \
        --project_id=fivetran-wild-west \
        --source_format=CSV \
        --skip_leading_rows=1 \
        --null_marker=. \
        mortality.deaths \
        $f
done
