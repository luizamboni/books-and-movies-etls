SELECT 
    round(median(size_mb) / 1000, 2) average_in_gigabyes
FROM 
    stream.streams