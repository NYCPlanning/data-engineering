/*
View: latest_version_status
 
This view selects the latest event for each product version 
from the event_logging table, excluding 'db-template'. 
It ranks events by priority ('publish', 'promote_to_draft', 'build') 
and timestamp, returning the top-ranked event.
*/

CREATE VIEW product_data.latest_version_status AS (
    WITH exclude_template AS (
        SELECT * 
        FROM product_data.event_logging 
        WHERE product <> 'db-template'
    ),
    ranked_events AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY product, version
                ORDER BY 
                    CASE 
                        WHEN event IN ('publish', 'promote_to_draft') THEN 1
                        WHEN event = 'build' THEN 2
                    END, 
                    timestamp DESC
            ) AS rank
        FROM exclude_template
    )
    SELECT 
        product,
        version,
        event,
        path,
        old_path,
        runner_type,
        runner,
        timestamp
    FROM ranked_events
    WHERE rank = 1
);
