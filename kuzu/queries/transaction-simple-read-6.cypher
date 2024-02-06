MATCH (src:Account {id: $id})<-[e1:AccountTransferAccount]-(mid:Account)-[e2:AccountTransferAccount]->(dst:Account {isBlocked: true})
WHERE src.id <> dst.id
  AND $startTime < e1.timestamp AND e1.timestamp < $endTime
  AND $startTime < e2.timestamp AND e2.timestamp < $endTime
WITH list_distinct(collect(dst.id)) AS dstIdList
UNWIND dstIdList AS dstId
RETURN dstId
ORDER BY dstId