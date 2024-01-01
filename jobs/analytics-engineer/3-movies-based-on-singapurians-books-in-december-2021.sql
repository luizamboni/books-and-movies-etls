WITH books_from_singaporeans_authors AS (
    SELECT 
        b.name book_name
    FROM
        vendor.books b
    JOIN
        vendor.authors a ON b.author = a.name
    WHERE 1=1
        AND array_contains(a.nationality_labels, 'singaporeans')
),

streamed_movies_on_december_2021 AS (
    SELECT 
        distinct movie_title
    FROM 
        stream.streams 
    WHERE 
        to_date(date_trunc('month', start_at)) = to_date('2021-12-01')
        OR     
        to_date(date_trunc('month', end_at)) = to_date('2021-12-01')
)

SELECT 
    count(1) quantity
FROM
    streamed_movies_on_december_2021 m 
JOIN 
    books_from_singaporeans_authors b ON m.movie_title = b.book_name
    
