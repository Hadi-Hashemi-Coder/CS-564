WITH numCats AS (SELECT ItemId, COUNT(Description) as ct
    FROM Categories
    GROUP BY ItemId)
SELECT Count(ItemId)
FROM numCats
WHERE numCats.ct = 4;