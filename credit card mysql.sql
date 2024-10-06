use explore

CREATE TABLE cc_detail (
    Client_Num INT,
    Card_Category VARCHAR(20),
    Annual_Fees INT,
    Activation_30_Days INT,
    Customer_Acq_Cost INT,
    Week_Start_Date DATE,
    Week_Num VARCHAR(20),
    Qtr VARCHAR(10),
    current_year INT,
    Credit_Limit DECIMAL(10,2),
    Total_Revolving_Bal INT,
    Total_Trans_Amt INT,
    Total_Trans_Ct INT,
    Avg_Utilization_Ratio DECIMAL(10,3),
    Use_Chip VARCHAR(10),
    Exp_Type VARCHAR(50),
    Interest_Earned DECIMAL(10,3),
    Delinquent_Acc VARCHAR(5)
);
SHOW VARIABLES LIKE 'secure_file_priv';
LOAD DATA  INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cc_add.csv'
INTO TABLE  cc_detail
FIELDS TERMINATED  BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
ignore 1 rows;

select * from explore.cc_detail

CREATE TABLE cust_detail (
    Client_Num INT,
    Customer_Age INT,
    Gender VARCHAR(5),
    Dependent_Count INT,
    Education_Level VARCHAR(50),
    Marital_Status VARCHAR(20),
    State_cd VARCHAR(50),
    Zipcode VARCHAR(20),
    Car_Owner VARCHAR(5),
    House_Owner VARCHAR(5),
    Personal_Loan VARCHAR(5),
    Contact VARCHAR(50),
    Customer_Job VARCHAR(50),
    Income INT,
    Cust_Satisfaction_Score INT
);


LOAD DATA  INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cust_add.csv'
INTO TABLE  cust_detail
FIELDS TERMINATED  BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
ignore 1 rows;

select * from cust_detail

select distinct trim(substr(week_num,6,2)) from cc_detail

Alter table cc_detail add column weeknum2 int

update cc_detail set  weeknum2=trim(substr(week_num,6,2) )

select * from cc_detail

select sum(total_sum) as Revenue from (
select Total_Trans_Amt + Interest_Earned + annual_fees as total_sum from cc_detail) A

select *,
(case when income<35000 then 'Low'
when income>=35000 and income<70000 then 'Med'
when income>=70000 then 'High' 
end) as IncomeGroup from cust_detail

create table cust_detail1 like cust_detail
Alter table cust_detail1 add column IncomeGroup text
Alter table cust_detail1 add column AgeGroup text

insert into cust_detail1 
select *,(case when income<35000 then 'Low'
when income>=35000 and income<70000 then 'Med'
when income>=70000 then 'High' 
end) as IncomeGroup from cust_detail

select *,
(case when customer_age<30 then '20-30'
when customer_age>=30 and customer_age<40 then '30-40'
when customer_age>=40 and customer_age<50 then '40-50' 
when customer_age>=50 and customer_age<60 then '50-60'
when customer_age>=60 then '60+'
else 'unknown'
end) as AgeGroup from cust_detail1

update cust_detail1 set AgeGroup=
(case when customer_age<30 then '20-30'
when customer_age>=30 and customer_age<40 then '30-40'
when customer_age>=40 and customer_age<50 then '40-50' 
when customer_age>=50 and customer_age<60 then '50-60'
when customer_age>=60 then '60+'
else 'unknown'
end) 
-- calculating revenue , currentweekrevenue and previousweekrevenue
with rev_cte as (
select weeknum2,week_num,sum(Total_Trans_Amt)+ sum(Interest_Earned) + sum(annual_fees) as revenue 
from cust_detail1 cd ,cc_detail cc 
where cc.client_num=cd.client_num 
group by weeknum2,week_num
)
select *,revenue as CurrentWeekRevenue,
lag(revenue,1,0) over(order by weeknum2) as PreviousWeekRevenue from rev_cte

select *, concat(round((((CurrentWeekRevenue - PreviousWeekRevenue)/PreviousWeekRevenue)*100) ,2),'%') as wow_revenue
from 
(with rev_cte as (
select weeknum2,week_num,sum(Total_Trans_Amt)+ sum(Interest_Earned) + sum(annual_fees) as revenue 
from cust_detail1 cd ,cc_detail cc 
where cc.client_num=cd.client_num 
group by weeknum2,week_num
)
select *,revenue as CurrentWeekRevenue,
lag(revenue,1,0) over(order by weeknum2) as PreviousWeekRevenue from rev_cte) a

-- total activation rate
select Activation_30_Days,gender, count(Activation_30_Days) from cc_detail cc, cust_Detail1 cd 
where cc.client_num=cd.client_num
group by Activation_30_Days,gender

select count(Activation_30_Days) as total_customers from cc_detail



with cte as (
select gender,
sum(case when Activation_30_Days=0 then 1 else 0 end) as inactive_customers,
sum(case when Activation_30_Days=1 then 1 else 0 end) as active_customers
from cc_detail cc,cust_Detail1 cd 
where cc.client_num=cd.client_num
group by Activation_30_Days,gender)

-- or 

with cte as (
select count(client_num) as total_customers,
sum(case when Activation_30_Days=0 then 1 end) as inactive_customers,
sum(case when Activation_30_Days=1 then 1 end) as active_customers
from cc_detail)

select count(cc.client_num) as total_customers,
sum(case when Activation_30_Days=0 then 1 end) over(partition by gender) as inactive_customers
from cc_detail cc , cust_detail1 cd where cc.client_num=cd.client_num 
group by 

/*
cc,cust_Detail1 cd 
where cc.client_num=cd.client_num
-- group by Activation_30_Days)
*/
-- select total_customers,sum(inactive_customers),active_customers from cte  group by 

