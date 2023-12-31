WITH books_and_movies AS (
    SELECT 
        IF(b.name is NULL, false, true) AS based_on_movie
    FROM 
        stream.movies m 
    LEFT JOIN 
        vendor.books b ON lower(m.title) = lower(b.name)
),


quantity_of_books_based_on_movies AS (
    SELECT 
        based_on_movie,
        count(1) quantity
    FROM 
        books_and_movies 
    GROUP BY 
        based_on_movie
),

total_of_books AS (
    SELECT 
        sum(quantity) total
    FROM 
        quantity_of_books_based_on_movies
)

SELECT
    q.based_on_movie,
    round(q.quantity / t.total * 100, 2) percentage
FROM 
    quantity_of_books_based_on_movies q
JOIN 
    total_of_books t ON 1=1


