/*
View: product_version_lifecycle

This query aggregates information from the event logging table per product + version.
It returns only product versions that have both publish and draft events,
including the count of published and draft records, the earliest draft 
timestamp, the latest publish timestamp, and the difference in days 
between the two timestamps.
 */

CREATE VIEW product_data.product_version_lifecycle AS (
    WITH latest_publish AS (
        SELECT 
            product, 
            version, 
            COUNT(*) AS publish_count, 
            MAX(timestamp) AS latest_publish_timestamp
        FROM product_data.event_logging
        WHERE product <> 'db-template' 
        AND event = 'publish'
        GROUP BY 
            product, 
            version
    ),
    earliest_draft AS (
        SELECT 
            product, 
            version, 
            COUNT(*) AS draft_count, 
            MIN(timestamp) AS earliest_draft_timestamp
        FROM product_data.event_logging
        WHERE product <> 'db-template'
        AND event = 'promote_to_draft'
        GROUP BY 
            product, 
            version
    )
    SELECT 
        published.product, 
        published.version, 
        published.publish_count, 
        draft.draft_count, 
        draft.earliest_draft_timestamp,
        published.latest_publish_timestamp, 
        DATE_PART('day', published.latest_publish_timestamp - draft.earliest_draft_timestamp) AS total_days
    FROM latest_publish AS published
    INNER JOIN earliest_draft AS draft
        ON published.product = draft.product 
        AND published.version = draft.version
    )
);
