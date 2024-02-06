MATCH (dst:Account {id: $id})
WITH dst
OPTIONAL MATCH (src:Account)-[edge2:AccountTransferAccount]->(dst)
WITH dst, count(edge2) AS edge2Num
OPTIONAL MATCH (blockedSrc:Account {isBlocked: true})-[edge1:AccountTransferAccount]->(dst)
WHERE 
  $startTime < edge1.timestamp AND edge1.timestamp < $endTime
  AND edge1.amount > $threshold
WITH count(edge1) AS edge1Num, edge2Num
WITH
  CASE edge2Num
    WHEN 0 THEN -1.0
    ELSE (1.0 * edge1Num) / edge2Num
  END AS blockRatio
RETURN CAST(round(blockRatio, 3), "FLOAT")