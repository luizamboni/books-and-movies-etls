WITH duration_of_streams_in_minutes AS (

    SELECT 
        round(CAST(to_timestamp(end_at) - to_timestamp(start_at) AS int) / 60, 2) minutes  
    FROM 
        stream.streams
)

SELECT 
    round(avg(minutes), 2) minutes
FROM 
    duration_of_streams_in_minutes
