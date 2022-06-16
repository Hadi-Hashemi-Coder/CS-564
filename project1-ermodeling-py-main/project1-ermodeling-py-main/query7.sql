select count(distinct c.Description)
from Categories c
INNER JOIN Bids b ON c.ItemId = b.ItemId
where b.Amount > 100;
