MATCH (src:Account {id: $id})
OPTIONAL MATCH (src)-[edge1:AccountTransferAccount]->(dst1:Account)
WHERE $startTime < edge1.timestamp AND edge1.timestamp < $endTime
OPTIONAL MATCH (src)<-[edge2:AccountTransferAccount]-(dst2:Account)
WHERE $startTime < edge2.timestamp AND edge2.timestamp < $endTime
WITH
	round(sum(edge1.amount), 3) AS sumEdge1Amount,
	count(edge1) AS numEdge1,
	round(sum(edge2.amount), 3) AS sumEdge2Amount,
	count(edge2) AS numEdge2,
	edge1, edge2
WITH
	sumEdge1Amount, numEdge1,
	sumEdge2Amount, numEdge2,
	CASE numEdge1
	  WHEN 0 THEN -1.0
	  ELSE round(max(edge1.amount), 3)
	END AS maxEdge1Amount,
	CASE numEdge2
	  WHEN 0 THEN -1.0
	  ELSE round(max(edge2.amount), 3)
	END AS maxEdge2Amount
RETURN
	sumEdge1Amount, maxEdge1Amount, numEdge1,
	sumEdge2Amount, maxEdge2Amount, numEdge2

