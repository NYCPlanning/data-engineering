SELECT
    TO_DATE(published_date, 'MM/DD/YYYY') AS published_date,
    financial_plan,
    -- project_id is the managing agency code and project ID separated by a space
    REGEXP_REPLACE(project_id, '^(\S+) ', '\1') AS maprojid,
    project_id,
    project_description,
    budget_line,
    budget_line_title,
    CASE
        WHEN asset_category = 'It & Technology' THEN 'IT & Technology'
        ELSE asset_category
    END AS asset_category,
    ghg_mitigation_tracking_category,
    flood_resiliency_tracking_category,
    heat_resiliency_tracking_category,
    LOWER(heat_vulnerability_index)::boolean AS heat_vulnerability_index,
    LOWER(flood_vulnerability_index)::boolean AS flood_vulnerability_index,
    ghg_mitigation,
    flood_resiliency,
    heat_resiliency,
    environmental_and_social_benefits,
    first_fiscal_year::int AS fiscal_year,
    fiscal_year_amount::numeric AS fiscal_year_amount,
    remarks
FROM {{ source('recipe_sources', 'omb_capital_climate_investments') }}
