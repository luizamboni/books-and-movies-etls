WITH books_from_singaporeans_authors AS (
    SELECT 
        b.name book_name,
        a.name author_name
    FROM
        vendor.books b
    JOIN
        vendor.authors a ON b.author = a.name
    WHERE 1=1
        AND array_contains(nationality_labels, 'Singaporeans')
),

streamed_movies_on_last_month AS (
    SELECT 
        movie_title,
        to_date(date_trunc('month', start_at)) start_month,
        to_date(date_trunc('month', end_at)) end_month
    FROM 
        stream.streams 
    WHERE 
        to_date(date_trunc('month', start_at)) = to_date(date_trunc('month', current_date() - INTERVAL 1 MONTH))
        OR     
        to_date(date_trunc('month', end_at)) = to_date(date_trunc('month', current_date() - INTERVAL 1 MONTH))
)

SELECT 
    count(1) quantity
FROM
    streamed_movies_on_last_month m 
JOIN 
    books_from_singaporeans_authors b ON m.movie_title = b.book_name
    
