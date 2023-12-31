SELECT 
    count(1) quantity
FROM 
    stream.streams 
WHERE 1=1 
    AND movie_title = 'Unforgiven'
    AND (
        (
            to_timestamp(start_at) >= to_timestamp('2021-12-25T07:00:00') 
            AND to_timestamp(start_at) <= to_timestamp('2021-12-25T12:00:00')
        )
        OR
        (
            to_timestamp(end_at) >= to_timestamp('2021-12-25T07:00:00') 
            AND to_timestamp(end_at) <= to_timestamp('2021-12-25T12:00:00')
        )
    )