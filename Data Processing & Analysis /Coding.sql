
select * from SNOWFLAKE_LEARNING_DB.PUBLIC.DATASET
limit 10;



--Create a lookup table for state abbreviations 
--CREATE TABLE OR REPLACE TABLE SNOWFLAKE_LEARNING_DB.PUBLIC.STATE_NAME_LOOKUP (abbrev STRING, full_name STRING);
INSERT INTO SNOWFLAKE_LEARNING_DB.PUBLIC.STATE_NAME_LOOKUP VALUES
('AB','Alberta'),
('AL','Alabama'),
('AZ','Arizona'),
('CA','California'),
('CO','Colorado'),
('FL','Florida'),
('GA','Georgia'),
('HI','Hawaii'),
('IL','Illinois'),
('IN','Indiana'),
('LA','Louisiana'),
('MA','Massachusetts'),
('MD','Maryland'),
('MI','Michigan'),
('MN','Minnesota'),
('MO','Missouri'),
('MS','Mississippi'),
('NC','North Carolina'),
('NE','Nebraska'),
('NJ','New Jersey'),
('NM','New Mexico'),
('NS','Nova Scotia'),
('NV','Nevada'),
('NY','New York'),
('OH','Ohio'),
('OK','Oklahoma'),
('ON','Ontario'),
('OR','Oregon'),
('PA','Pennsylvania'),
('PR','Puerto Rico'),
('QC','Quebec'),
('SC','South Carolina'),
('TN','Tennessee'),
('TX','Texas'),
('UT','Utah'),
('VA','Virginia'),
('WA','Washington'),
('WI','Wisconsin');

--Create table SNOWFLAKE_LEARNING_DB.PUBLIC.CAR_MAKE_LOOKUP(raw_make STRING,canonical_make STRING); 
Insert Into  SNOWFLAKE_LEARNING_DB.PUBLIC.CAR_MAKE_LOOKUP VALUES ('Chevrolet', 'Chevrolet'), 
('chev truck', 'Chevrolet'), 
('Dodge', 'Dodge'), ('dodge tk', 'Dodge'), 
('Ford', 'Ford'), ('ford tk', 'Ford'), 
('ford truck', 'Ford'), 
('Hyundai', 'Hyundai'), 
('hyundai tk', 'Hyundai'), 
('Land Rover', 'Land Rover'), 
('landrover', 'Land Rover'), 
('Mazda', 'Mazda'), 
('mazda tk', 'Mazda'), 
('mercedes', 'Mercedes'), 
('mercedes-b', 'Mercedes'), 
('Mercedes-Benz', 'Mercedes'), 
('Volkswagen', 'Volkswagen'), 
('vw','Volkswagen');

--Creating a permanent virtual table of cleaned data 
create or replace view snowflake_learning_db.public.base AS
SELECT year,  
    -- Normalized make for joining
    REGEXP_REPLACE(UPPER(TRIM(make)), '[^A-Z0-9 ]', '') AS normalized_make,
    case when make is null or trim(make) = '' then 'Unknown' else initcap(make) end as car_make,
    case when model is null or trim(model) = '' then 'Unknown' else initcap(model) end as model_type, 
    case when body is null or trim(body) = '' then 'Unknown' else initcap(body)end as body_type, 
    case when transmission is NULL or trim(transmission) =  '' then 'Unknown' else initcap(transmission) end as transmission_cleaned,
    state,
    case 
        when color is null or trim(color) = '' then 'Unknown'
        when color = '—' then 'Unknown'
        when regexp_like(color,'^[0-9]+$') then 'Unknown'
        else initcap(color) end as car_color,
    case
        when interior is null or trim(interior) = '' then 'Unknown'
        when interior = '—' then 'Unknown'
        else initcap(interior) end as car_interior,
    initcap(seller) as seller_name,

/*
Check the number of seller | 14263
select count(distinct seller) as distinct_sellers from SNOWFLAKE_LEARNING_DB.PUBLIC.DATASET
*/

    condition,   
    case
        when condition between 1 and 19 then 'Very Poor Condition'
        when condition between 20 and 29 then 'Poor Condition'
        when condition between 30 and 39 then 'Average Condition'
        when condition between 40 and 49 then 'Good Condition'
        else 'Excellent Condition' end as Condition_State, 
    
    odometer,
     case 
        when odometer between 1 and 19999 then 'Low Mileage'
        when odometer between  20000 and 59999 then 'Moderate Mileage'
        when odometer between 60000 and 119999 then 'High Mileage'
        when odometer between 120000 and 199999 then 'Very High Mileage'
        else 'Extreme Mileage'
    end as Odometer_Category, 
    
    --Profit Margins 
        mmr, sellingprice, 
        (((sellingprice - mmr) / sellingprice) * 100) as avg_profit_margin,

    
        try_to_timestamp(left(saledate,24), 'DY MON DD YYYY HH24:MI:SS') as sale_stamp,
        dayname(try_to_timestamp(left(saledate,24),'DY MON DD YYYY HH24:MI:SS' )) as sale_day,
        monthname(try_to_timestamp(left(saledate,24),'DY MON DD YYYY HH24:MI:SS')) as sale_month,
        year(try_to_timestamp(left(saledate,24),'DY MON DD YYYY HH24:MI:SS')) as sale_year,  

    FROM SNOWFLAKE_LEARNING_DB.PUBLIC.DATASET
    where not regexp_like(saledate,'^[0-9]+$');


select 
    b.year,
    COALESCE(lkp.canonical_make, b.car_make) AS standardized_make,
    b.model_type, 
    b.body_type,
    b.transmission_cleaned,
    COALESCE(sl.FULL_NAME, 'Unknown') as state_name,
    b.car_color,
    b.car_interior, 
    b.seller_name,
    b.condition,
    b.condition_state,
    b.odometer,
    b.Odometer_Category,
    b.mmr,
    b.sellingprice,
    b.avg_profit_margin,
    Case
        when b.avg_profit_margin < 0 then 'Loss'
        when b.avg_profit_margin between 0 and 10 then 'Low Margin'
        when b.avg_profit_margin between 10 and 25 then 'Moderate Margin'
        else 'High Margin' end as profit_category, 

  b.sale_stamp,
  b.sale_day,
  b.sale_month, 
  b.sale_year,

    case
        when b.sale_day in ('Saturday', 'Sunday') then 'Weekend'
        else 'Weekday'
    end as day_classification,
 
From SNOWFLAKE_LEARNING_DB.PUBLIC.base b
LEFT JOIN SNOWFLAKE_LEARNING_DB.PUBLIC.STATE_NAME_LOOKUP sl
    On UPPER(b.state) = sl.ABBREV
Left Join SNOWFLAKE_LEARNING_DB.PUBLIC.CAR_MAKE_LOOKUP lkp
    On b.normalized_make = lkp.raw_make;

--EXTRACTING INSIGHTS 
-- Top Car Make by Profit Margin 
Create or replace view SNOWFLAKE_LEARNING_DB.PUBLIC.car_make_insights as 
SELECT 
    COALESCE(lkp.canonical_make, b.car_make) AS standardized_make,
    AVG(b.avg_profit_margin) AS avg_margin,
    COUNT(*) AS total_sales
FROM SNOWFLAKE_LEARNING_DB.PUBLIC.base b
LEFT JOIN SNOWFLAKE_LEARNING_DB.PUBLIC.CAR_MAKE_LOOKUP lkp
    ON b.normalized_make = lkp.raw_make
GROUP BY standardized_make
ORDER BY avg_margin DESC;

select * 
from SNOWFLAKE_LEARNING_DB.PUBLIC.car_make_insights;

--Sales by State 
create or replace view SNOWFLAKE_LEARNING_DB.PUBLIC.state_sales as
select  
    coalesce(sl.full_name, 'Unknown') as state_name,
    count(*) as total_sales, 
    avg(b.sellingprice) as avg_price
from snowflake_learning_db.public.base b
left join snowflake_learning_db.public.state_name_lookup sl
    on upper(b.state) = sl.abbrev
group by state_name
order by total_sales DESC;

select *
from snowflake_learning_db.public.state_sales; --SNOWFLAKE_LEARNING_DB.PUBLIC.state_sales;
    

--Weekend vs Weekday Performance 
create or replace view snowflake_learning_db.public.week_performance as
select
    case when b.sale_day in ('Saturday', 'Sunday') then 'Weekend' else 'Weekday' end as day_classification,
    avg(b.sellingprice) as avg_price,
    avg(b.avg_profit_margin) as avg_margin,
    count(*) as total_sales
from snowflake_learning_db.public.base b
group by day_classification;

select *
from snowflake_learning_db.public.week_performance;
    
    
--Condition vs Profitability 
create or replace view snowflake_learning_db.public.condition_vs_profitability as
select 
        b.condition_state, 
        avg(b.avg_profit_margin) as avg_margin,
        case    
            when b.avg_profit_margin < 0 then 'Loss'
            when b.avg_profit_margin between 0 and 10 then 'Low Margin'
            when b.avg_profit_margin between 10 and 25 then 'Moderate Margin'
            else 'High Margin' end as profit_category, 
    count(*) as total_sales
from snowflake_learning_db.public.base b
group by b.condition_state, profit_category
order by profit_category desc; 

select *
from snowflake_learning_db.public.condition_vs_profitability; 

--Seller Performance 
create or replace view snowflake_learning_db.public.seller_performance as
select b.seller_name, sum(b.sellingprice) as selling_price
from snowflake_learning_db.public.base b
group by b.seller_name
order by selling_price desc;

select * 
from snowflake_learning_db.public.seller_performance;
