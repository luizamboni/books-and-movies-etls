WITH users_that_watched_movies_last_week AS (
    SELECT 
        s.user_email, 
        s.movie_title,
        round(CAST(to_timestamp(s.end_at) - to_timestamp(s.start_at) AS int) / 60, 2) minutes_watched, 
        m.duration_mins minutes_of_movie
    FROM 
        stream.streams s
    JOIN 
        stream.movies m ON lower(s.movie_title) = lower(m.title)
    WHERE 1=1
        AND to_timestamp(start_at) >= current_date() - INTERVAL 7 days
),

unique_users_that_wathet_at_least_one_movie_more_than_50 AS (
    SELECT 
        distinct user_email
    FROM
        users_that_watched_movies_last_week
    WHERE 1=1
        AND minutes_of_movie / minutes_watched > 0.5
)

SELECT 
    COUNT(1) quantity
FROM 
    unique_users_that_wathet_at_least_one_movie_more_than_50

