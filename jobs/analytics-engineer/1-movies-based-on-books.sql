WITH movies_based_on_books AS (
    SELECT 
        IF(b.name is NULL, false, true) AS based_on_movie
    FROM 
        stream.movies m 
    LEFT JOIN 
        vendor.books b ON m.title = b.name
),


quantity_of_movies_based_on_books AS (
    SELECT 
        based_on_movie,
        count(1) quantity
    FROM 
        movies_based_on_books 
    GROUP BY 
        based_on_movie
),

total_of_books AS (
    SELECT 
        sum(quantity) total
    FROM 
        quantity_of_movies_based_on_books
)

SELECT
    q.based_on_movie based_on_movie,
    round(q.quantity / t.total * 100, 2) percentage
FROM 
    quantity_of_movies_based_on_books q
JOIN 
    total_of_books t ON 1=1


