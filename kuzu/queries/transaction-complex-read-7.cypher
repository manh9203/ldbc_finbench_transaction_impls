MATCH (mid:Account {id: $id})
WITH mid
OPTIONAL MATCH (src:Account)-[edge1:AccountTransferAccount]->(mid)
WHERE 
  $startTime < edge1.timestamp AND edge1.timestamp < $endTime 
  AND edge1.amount > $threshold
WITH 
  mid, 
  count(src) AS numSrc, 
  sum(edge1.amount) AS amountSrc
OPTIONAL MATCH (mid)-[edge2:AccountTransferAccount]->(dst:Account)
WHERE 
  $startTime < edge2.timestamp AND edge2.timestamp < $endTime 
  AND edge2.amount > $threshold
WITH 
  numSrc, amountSrc, 
  count(dst) AS numDst, 
  sum(edge2.amount) AS amountDst
WITH 
  numSrc, numDst,
  CASE numDst
    WHEN 0 THEN -1.0
    ELSE (1.0 * amountSrc) / amountDst
  END AS inOutRatio
RETURN 
  CAST(numSrc, "INT32"),
  CAST(numDst, "INT32"),
  CAST(round(inOutRatio, 3), "FLOAT")
