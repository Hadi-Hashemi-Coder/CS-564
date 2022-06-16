
select count(distinct i.SellerId)
from Items i
INNER JOIN Users ON Users.UserId = i.SellerId
WHERE Users.Rating > 1000;