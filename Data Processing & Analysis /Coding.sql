/*
select * from SNOWFLAKE_LEARNING_DB.PUBLIC.DATASET
limit 10;
*/

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

with base AS 
(
SELECT year,  
    make,  
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

    FROM
      SNOWFLAKE_LEARNING_DB.PUBLIC.DATASET
    where not regexp_like(saledate,'^[0-9]+$')
)

select 
    b.year,
    b.make, 
    b.model_type, 
    b.body_type,
    b.transmission_cleaned,
    COALESCE(sl.FULL_NAME, 'Unknown') as state_name,
    b.car_color,
    b.car_interior, 
    b.seller_name,
    b.condition,
    b.Condition_State,
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
 
From base b
LEFT JOIN SNOWFLAKE_LEARNING_DB.PUBLIC.STATE_NAME_LOOKUP sl
    On UPPER(b.state) = sl.ABBREV;
