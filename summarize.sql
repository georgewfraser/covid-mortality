-- https://www.mortality.org/Public/Docs/InputDBdoc.pdf
-- https://www.mortality.org/Public/HMD-countries-codes.pdf

create temp function implied_population(deaths float64, death_rate float64) as (
    if(deaths = 0 or death_rate = 0, null, deaths / death_rate * 52)
);

create or replace table `fivetran-wild-west.mortality.annual_mortality_recent` as
select 
    country_code,
    date_trunc(date_add(date, interval weekday day), year) as year,
    age_group,
    sex,
    cast(sum(deaths/7) as int64) as deaths,
    cast(avg(implied_population(deaths, death_rate)) as int64) as population,
from `fivetran-wild-west`.mortality.stmf
join unnest(array<struct<age_group string, deaths float64, death_rate float64>>[
    ('0-14', d0_14, r0_14), 
    ('15-64', d15_64, r15_64), 
    ('65-74', d65_74, r65_74), 
    ('75-84', d75_84, r75_84), 
    ('85+', d85p, r85p)
])
join (
    select extract(isoyear from date) as year, extract(isoweek from date) as week, date
    from unnest(generate_date_array('1990-01-01', '2021-01-01', interval 1 week)) as date
) as iso_weeks using (year, week)
join unnest([0, 1, 2, 3, 4, 5, 6]) as weekday
where sex in ('m', 'f')
group by 1, 2, 3, 4
order by 1, 2, 3, 4;

create or replace table `fivetran-wild-west.mortality.weekly_mortality_recent` as
select 
    country_code,
    date,
    age_group,
    sex,
    cast(sum(deaths) as int64) as deaths,
    cast(avg(implied_population(deaths, death_rate)) as int64) as population,
from `fivetran-wild-west`.mortality.stmf
join unnest(array<struct<age_group string, deaths float64, death_rate float64>>[
    ('0-14', d0_14, r0_14), 
    ('15-64', d15_64, r15_64), 
    ('65-74', d65_74, r65_74), 
    ('75-84', d75_84, r75_84), 
    ('85+', d85p, r85p)
])
join (
    select extract(isoyear from date) as year, extract(isoweek from date) as week, date
    from unnest(generate_date_array('1990-01-01', '2021-01-01', interval 1 week)) as date
) as iso_weeks using (year, week)
where sex in ('m', 'f')
group by 1, 2, 3, 4
order by 1, 2, 3, 4;

create temp function parse_age(i string) as (
    case i 
        when 'UNK' then null 
        when 'TOT' then null 
        else cast(i as int64) 
    end
);
create temp function parse_age_interval(i string) as (
    case i 
        when '+' then 100
        else cast(i as int64) 
    end
);
create temp function age_group (age string, age_interval string) as (
    case 
        when age_interval = '+' and parse_age(age) >= 85 then '85+'
        when parse_age(age) between 0 and 14 and parse_age(age)+parse_age_interval(age_interval)-1 between 0 and 14 then '0-14'
        when parse_age(age) between 15 and 64 and parse_age(age)+parse_age_interval(age_interval)-1 between 15 and 64 then '15-64'
        when parse_age(age) between 65 and 74 and parse_age(age)+parse_age_interval(age_interval)-1 between 65 and 74 then '65-74'
        when parse_age(age) between 75 and 84 and parse_age(age)+parse_age_interval(age_interval)-1 between 75 and 84 then '75-84'
        when parse_age(age) >= 85 then '85+'
        else null
    end
);

create or replace table `fivetran-wild-west.mortality.annual_mortality_historical` as 
select 
    pop_name as country_code,
    date(year, 1, 1) as year,
    age_group(age, age_interval) as age_group,
    sex,
    cast(sum(deaths.deaths) as int64) as deaths,
from `fivetran-wild-west`.mortality.deaths
where year_interval = 1
and sex in ('m', 'f')
and age not in ('UNK', 'TOT')
and ldb = '1'
group by 1, 2, 3, 4
order by 1, 2, 3, 4;

create temp function fix_date(country_code string, year int64, month int64, day int64) as (
    case 
        when country_code = 'CHE' and date(year, month, day) in ('2002-01-01', '2003-01-01', '2004-01-01', '2005-01-01', '2006-01-01', '2007-01-01', '2008-01-01', '2009-01-01') then date_sub(date(year, month, day), interval 1 day)
        else date(year, month, day)
    end
);

create or replace table `fivetran-wild-west.mortality.annual_population_historical` as 
with observations as (
    select 
        pop_name as country_code, 
        fix_date(pop_name, year, month, day) as date, 
        age_group(age, age_interval) as age_group, 
        sex,
        cast(sum(population.population) as int64) as population,
    from `fivetran-wild-west`.mortality.population
    where sex in ('m', 'f') 
    and age not in ('UNK', 'TOT')
    and ldb = '1'
    group by 1, 2, 3, 4
)
select 
    country_code, 
    age_group, 
    sex,  
    date as prev_population_date,
    ifnull(date_sub(lead(date) over country_age_sex, interval 1 day), date '3000-01-01') as next_population_date,
    population as prev_population,
    ifnull(lead(population) over country_age_sex, population) as next_population,
from observations
window country_age_sex as (partition by country_code, age_group, sex order by date)
order by 1, 2, 3, 4;

create temp function interpolate(x date, x1 date, x2 date, y1 int64, y2 int64) as (
    date_diff(x, x1, day) / date_diff(x2, x1, day) * y2 + 
    date_diff(x2, x, day) / date_diff(x2, x1, day) * y1
);

create or replace table `fivetran-wild-west.mortality.annual_summary` as
select 
    country_code, year, age_group, sex,
    if(annual_mortality_historical.deaths is not null, 
        annual_mortality_historical.deaths, 
        annual_mortality_recent.deaths
    ) as deaths,
    if(annual_mortality_historical.deaths is not null, 
        interpolate(year, prev_population_date, next_population_date, prev_population, next_population), 
        annual_mortality_recent.population
    ) as population,
from unnest(generate_date_array('1900-01-01', '2020-01-01', interval 1 year)) as year, 
    unnest(['AUT', 'BEL', 'BGR', 'CAN', 'CHE', 'CHL', 'CZE', 'DEUTNP', 'DNK', 'ESP', 'EST', 'FIN', 'FRATNP', 'GBRTENW', 'GRC', 'HRV', 'HUN', 'ISL', 'ISR', 'ITA', 'KOR', 'LTU', 'LUX', 'LVA', 'NLD', 'NOR', 'NZL_NP', 'POL', 'PRT', 'SVK', 'SVN', 'SWE', 'TWN', 'USA']) as country_code,
    unnest(['0-14', '15-64', '65-74', '75-84', '85+']) as age_group,
    unnest(['m', 'f']) as sex
left join `fivetran-wild-west`.mortality.annual_population_historical using (country_code, age_group, sex) 
left join `fivetran-wild-west`.mortality.annual_mortality_historical using (country_code, year, age_group, sex)
left join `fivetran-wild-west`.mortality.annual_mortality_recent using (country_code, year, age_group, sex)
where year between prev_population_date and next_population_date or prev_population_date is null
order by country_code, year, age_group, sex;

-- Age normalization constants.
select 
    round(sum(if(age_group = '0-14', population, 0)) / sum(population), 3),
    round(sum(if(age_group = '15-64', population, 0)) / sum(population), 3),
    round(sum(if(age_group = '65-74', population, 0)) / sum(population), 3),
    round(sum(if(age_group = '75-84', population, 0)) / sum(population), 3),
    round(sum(if(age_group = '85+', population, 0)) / sum(population), 3),
from `fivetran-wild-west.mortality.annual_summary` 
where extract(year from year) = 2020 
order by 1

-- Tableau formula
0.164 * SUM(IF [Age Group] = '0-14' THEN [Deaths] ELSE 0 END) / SUM(IF [Age Group] = '0-14' THEN [Population] ELSE 0 END) +
0.650 * SUM(IF [Age Group] = '15-64' THEN [Deaths] ELSE 0 END) / SUM(IF [Age Group] = '15-64' THEN [Population] ELSE 0 END) +
0.103 * SUM(IF [Age Group] = '65-74' THEN [Deaths] ELSE 0 END) / SUM(IF [Age Group] = '65-74' THEN [Population] ELSE 0 END) +
0.059 * SUM(IF [Age Group] = '75-84' THEN [Deaths] ELSE 0 END) / SUM(IF [Age Group] = '75-84' THEN [Population] ELSE 0 END) +
0.024 * SUM(IF [Age Group] = '85+' THEN [Deaths] ELSE 0 END) / SUM(IF [Age Group] = '85+' THEN [Population] ELSE 0 END)

-- Replaced by ldb = true
-- where type = case pop_name 
--     -- switches from O to R 2002-01-01
--     when 'DNK' then if(date < '2002-01-01', 'O', 'R')
--     -- switches from O to R 1990-01-01
--     when 'FIN' then if(date < '1990-01-01', 'O', 'R')
--     -- switches from O to R 2002-01-01
--     when 'ISL' then if(date < '2002-01-01', 'O', 'R')
--     -- switches from E to R 1997-01-01
--     when 'NOR' then if(date < '1997-01-01', 'E', 'R')
--     -- switches from O to R 1970-12-31, O stops 1992-12-31
--     when 'SWE' then if(date < '1970-12-31', 'O', 'R')
--     -- switches from O to E 1940-07-01, from E to O 1970-07-01
--     when 'USA' then case when date < '1940-07-01' then 'O' when date < '1970-07-01' then 'E' else 'O' end
--     -- other countries consistently report type 'O'
--     else 'O'
-- end
-- and case pop_name 
--     when 'TWN' then area = '20'
--     when 'RUS' then area = '1'
--     else true
-- end