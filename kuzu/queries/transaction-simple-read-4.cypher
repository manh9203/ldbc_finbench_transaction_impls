MATCH (src:Account {id: $id})-[edge:AccountTransferAccount]->(dst:Account)
WHERE 
  $startTime < edge.timestamp AND edge.timestamp < $endTime
  AND edge.amount > $threshold
RETURN dst.id AS dstId, CAST(count(edge), "INT32") AS numEdges, round(sum(edge.amount), 3) AS sumAmount
ORDER BY sumAmount DESC, dstId
