BQ_DATASET = "iowa_liquor"

BEGIN_SALES_YEAR = 2012
END_SALES_YEAR = 2021

DDL_DM_TOTAL_COUNTY_SALE= f"""
create or replace table {BQ_DATASET}.dm_total_county_sale as
    with
    cte_avg_county_population as (
        select
            p.county,
            p.fips,
            round(avg(p.population)) as avg_population,
            round(sum(p.population)) as sum_population
        from
            {BQ_DATASET}.dim_county_population p
        where
            p.`year` between {BEGIN_SALES_YEAR} and {END_SALES_YEAR}
        group by
            1, 2
    ),
    cte_total_enriched_with_population as (
        select
            s.county, 
            p.avg_population,
            p.sum_population,
            p.fips,
            round(sum(s.volume_sold_liters)) as total_sold_liters,
            round(sum(s.sale_dollars)) as total_sale_dollars
        from
            {BQ_DATASET}.fact_liquor_sale s 
            join cte_avg_county_population p on s.county = p.county
        where
            extract(year from s.sale_date) between {BEGIN_SALES_YEAR} and {END_SALES_YEAR}
        group by
            1, 2, 3, 4       
    ),
    cte_county_category_total as (
        select
            s.county,
            s.category_name,
            round(sum(s.volume_sold_liters)) as total_sold_liters,
            row_number() over(partition by s.county order by sum(s.volume_sold_liters) desc) as county_row_order_desc
        from    
            {BQ_DATASET}.fact_liquor_sale s
        group by
            1, 2
    )
    select
        s.county, 
        s.avg_population,
        s.fips,
        s.total_sold_liters,
        s.total_sale_dollars,
        round(s.total_sold_liters / s.sum_population, 2) as liters_per_person,
        t.category_name
    from
        cte_total_enriched_with_population s
        join cte_county_category_total t on t.county = s.county
            and county_row_order_desc = 1
"""

DDL_DM_TOTAL_YEAR_SALE = f"""
create or replace table {BQ_DATASET}.dm_total_year_sale as
    select
        extract(year from s.sale_date) as sale_year,
        round(sum(s.volume_sold_liters)) as total_sold_liters
    from
        {BQ_DATASET}.fact_liquor_sale s
    where
        extract(year from s.sale_date) between {BEGIN_SALES_YEAR} and {END_SALES_YEAR}
    group by
        1
"""

DDL_DM_TOTAL_CATEGORY_SALE = f"""
create or replace table {BQ_DATASET}.dm_total_category_sale as
    select
        s.category_name,
        round(sum(s.volume_sold_liters)) as total_sold_liters
    from
        {BQ_DATASET}.fact_liquor_sale s
    where
        extract(year from s.sale_date) between {BEGIN_SALES_YEAR} and {END_SALES_YEAR}
    group by
        1
"""