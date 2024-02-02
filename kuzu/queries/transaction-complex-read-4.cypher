MATCH
  (src:Account {id: $id1})-[edge1:AccountTransferAccount]->(dst:Account {id: $id2}),
	(dst)-[edge3:AccountTransferAccount]->(other:Account)-[edge2:AccountTransferAccount]->(src)
WHERE	$startTime < edge1.timestamp AND edge1.timestamp < $endTime
	AND $startTime < edge2.timestamp AND edge2.timestamp < $endTime
	AND $startTime < edge3.timestamp AND edge3.timestamp < $endTime
WITH
  other.id AS otherId,
  count(edge2) AS numEdge2, sum(edge2.amount) AS sumEdge2Amount, max(edge2.amount) AS maxEdge2Amount,
  count(edge3) AS numEdge3, sum(edge3.amount) AS sumEdge3Amount, max(edge3.amount) AS maxEdge3Amount
RETURN otherId, numEdge2, sumEdge2Amount, maxEdge2Amount, numEdge3, sumEdge3Amount, maxEdge3Amount
ORDER BY sumEdge2Amount DESC, sumEdge3Amount DESC, otherId;