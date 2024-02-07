BEGIN
QUERY
MATCH (src:Person {id: $srcId}), (dst:Person {id: $dstId})
RETURN CASE WHEN src.isBlocked = true OR dst.isBlocked = true THEN false ELSE true END AS isSuccess
QUERY
MATCH (src:Person {id: $srcId}), (dst:Person {id: $dstId})
CREATE (dst)<-[:PersonGuaranteePerson {timestamp: $time}]-(src)
RETURN true AS isSuccess
QUERY
MATCH 
	p = (:Person {id: $id})-[guarantee:PersonGuaranteePerson*1..5 (r, n | WHERE $startTime < r.timestamp AND r.timestamp < $endTime)]->(:Person)
UNWIND nodes(p)[2:size(nodes(p))+1] AS person
MATCH (:Person {id: person.id})-[:PersonApplyLoan]->(loan:Loan)
WITH round(sum(loan.loanAmount), 3) AS sumLoanAmount
RETURN 
  CASE 
    WHEN sumLoanAmount <= $threshold THEN true 
    ELSE false 
  END AS isSuccess
COMMIT

BEGIN
QUERY
MATCH (src:Person {id: $srcId}), (dst:Person {id: $dstId})
SET src.isBlocked = true,  dst.isBlocked = true
RETURN true AS isSuccess
COMMIT