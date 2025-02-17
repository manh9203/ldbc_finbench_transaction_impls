CREATE NODE TABLE Person(id INT64, name STRING, isBlocked BOOLEAN, createTime TIMESTAMP, gender STRING, birthday TIMESTAMP, country STRING, city STRING, PRIMARY KEY(id));
CREATE NODE TABLE Company(id INT64, name STRING, isBlocked BOOLEAN, createTime TIMESTAMP, country STRING, city STRING, business STRING, description STRING, url STRING, PRIMARY KEY(id));
CREATE NODE TABLE Account(id INT64, createTime TIMESTAMP, isBlocked BOOLEAN, type STRING, nickname STRING, phoneNumber STRING, email STRING, freqLoginTime STRING, lastLoginTime STRING, accountLevel STRING, PRIMARY KEY(id));
CREATE NODE TABLE Loan(id INT64, loanAmount DOUBLE, balance DOUBLE, createTime TIMESTAMP, usage STRING, interestRate FLOAT, PRIMARY KEY(id));
CREATE NODE TABLE Medium(id INT64, type STRING, isBlocked BOOLEAN, createTime TIMESTAMP, lastLoginTime STRING, riskLevel STRING, PRIMARY KEY(id));
CREATE REL TABLE AccountTransferAccount(FROM Account TO Account, amount DOUBLE, timestamp TIMESTAMP, ordernumber STRING, comment STRING, payType STRING, goodsType STRING, MANY_MANY);
CREATE REL TABLE AccountWithdrawAccount(FROM Account TO Account, amount DOUBLE, timestamp TIMESTAMP, MANY_MANY);
CREATE REL TABLE AccountRepayLoan(FROM Account to Loan, amount DOUBLE, timestamp TIMESTAMP, MANY_MANY);
CREATE REL TABLE LoanDepositAccount(FROM Loan TO Account, amount DOUBLE, timestamp TIMESTAMP, MANY_MANY);
CREATE REL TABLE MediumSignInAccount(FROM Medium TO Account, timestamp TIMESTAMP, location STRING, MANY_MANY);
CREATE REL TABLE PersonInvestCompany(FROM Person TO Company, ratio FLOAT, timestamp TIMESTAMP, MANY_MANY);
CREATE REL TABLE CompanyInvestCompany(FROM Company TO Company, ratio FLOAT, timestamp TIMESTAMP, MANY_MANY);
CREATE REL TABLE PersonApplyLoan(FROM Person TO Loan, timestamp TIMESTAMP, organization STRING, ONE_MANY);
CREATE REL TABLE CompanyApplyLoan(FROM Company TO Loan, timestamp TIMESTAMP, organization STRING, ONE_MANY);
CREATE REL TABLE PersonGuaranteePerson(FROM Person TO Person, timestamp TIMESTAMP, relationship STRING, MANY_MANY);
CREATE REL TABLE CompanyGuaranteeCompany(FROM Company TO Company, timestamp TIMESTAMP, relationship STRING, MANY_MANY);
CREATE REL TABLE PersonOwnAccount(FROM Person TO Account, timestamp TIMESTAMP, ONE_MANY);
CREATE REL TABLE CompanyOwnAccount(FROM Company TO Account, timestamp TIMESTAMP, ONE_MANY);
