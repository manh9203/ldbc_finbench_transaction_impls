MATCH 
	p = (:Person {id: $id})-[guarantee:PersonGuaranteePerson*1..5 (r, n | WHERE $startTime < r.timestamp AND r.timestamp < $endTime)]->(:Person)
UNWIND nodes(p)[2:size(nodes(p))+1] AS person
MATCH (:Person {id: person.id})-[:PersonApplyLoan]->(loan:Loan)
RETURN 
	round(sum(loan.loanAmount), 3) AS sumLoanAmount, 
	CAST(count(loan), "INT32") AS numLoans
