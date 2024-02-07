MATCH 
  (person:Person {id: $id})-[edge1:PersonOwnAccount]->(pAcc:Account)
  -[edge2:AccountTransferAccount]->(compAcc:Account)
  <-[edge3:CompanyOwnAccount]-(company:Company)
WHERE $startTime < edge2.timestamp AND edge2.timestamp < $endTime
RETURN compAcc.id AS compAccountId, round(sum(edge2.amount), 3) AS sumEdge2Amount
ORDER BY sumEdge2Amount DESC, compAccountId
