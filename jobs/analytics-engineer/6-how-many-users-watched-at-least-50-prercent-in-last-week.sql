WITH last_day_streamed AS (
    SELECT 
        max(end_at) last_streamed_day
    FROM 
        stream.streams
),

users_that_watched_movies_last_week AS (
    SELECT 
        s.user_email, 
        s.movie_title,
        round(CAST(s.end_at - s.start_at AS int) / 60, 2) minutes_watched, 
        m.duration_mins minutes_of_movie
    FROM 
        stream.streams s
    JOIN 
        stream.movies m ON s.movie_title = m.title
    JOIN 
        last_day_streamed ls ON 1=1
    WHERE 1=1
        AND start_at >= ls.last_streamed_day - INTERVAL 7 days
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

