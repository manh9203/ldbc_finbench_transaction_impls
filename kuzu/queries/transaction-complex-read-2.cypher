MATCH 
    (person:Person {id: $id})-[own:PersonOwnAccount]->(account:Account),
    p = (other:Account)-[transfer:AccountTransferAccount*1..3 (r, n | WHERE $startTime < r.timestamp AND r.timestamp < $endTime)]->(account),
    (other)<-[deposit:LoanDepositAccount]-(loan:Loan)
WITH
    deposit.timestamp AS depositTime,
    properties(rels(p), 'timestamp') AS ts,
    other, loan
WHERE
    $startTime < depositTime AND depositTime < $endTime
    AND ts = list_sort(ts, 'ASC')
RETURN other.id AS otherId, round(sum(loan.loanAmount), 3) AS sumLoanAmount, round(sum(loan.balance), 3) AS sumLoanBalance
ORDER BY sumLoanAmount DESC, otherId
