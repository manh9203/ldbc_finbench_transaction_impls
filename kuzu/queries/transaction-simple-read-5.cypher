MATCH (dst:Account {id: $id})<-[edge:AccountTransferAccount]-(src:Account)
WHERE 
  $startTime < edge.timestamp AND edge.timestamp < $endTime
  AND edge.amount > $threshold
RETURN src.id AS srcId, CAST(count(edge), "INT32") AS numEdges, round(sum(edge.amount), 3) AS sumAmount
ORDER BY sumAmount DESC, srcId
