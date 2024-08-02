-- Data Cleaning

select *
from layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or blank values
-- 4. Remove Any Columns or Rows



create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

-- 1. Removing Duplicates
-- a. Making row_num column
select *,
row_number() over(partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs_staging;

-- b. Search row_num > 1
with duplicate_cte as
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

select *
from layoffs_staging
where company ='Casper';

-- c. This method won't work, instead using another method (read below)
with duplicate_cte as
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
delete
from duplicate_cte
where row_num > 1;

-- d. Create another table with the same column and data type
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2;

-- e. Copy all the column into the new table
insert into layoffs_staging2
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

select *
from layoffs_staging2
where row_num > 1;

-- f. Now, delete the duplicate data
delete
from layoffs_staging2
where row_num > 1;


-- 2. Standardizing Data

-- a. TRIM the `company` column. TRIM means take offs white space off the end.
select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

-- b. Change another 'Crypto%' name into 'Crypto'
select distinct industry
from layoffs_staging2
order by 1;

select *
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

-- b. Fix the country name (United States)
select distinct country
from layoffs_staging2
order by 1;

-- TRAILING used to remove some character at the end of the string. In this case, '.'
select distinct country, TRIM(TRAILING '.' FROM country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = TRIM(TRAILING '.' FROM country)
where country like 'United States%';

-- c. Change the data type of the `date`
select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y')
;

select `date`
from layoffs_staging2;

alter table layoffs_staging2
modify column `date` date;


-- 3. Null Values and Blank Values

select *
from layoffs_staging2
where total_laid_off is NULL
and percentage_laid_off is NULL
;

-- a. On the industry column, replace blank ('') column with NULL
update layoffs_staging2
set industry = NULL
where industry = '';

select *
from layoffs_staging2
where industry is NULL 
or industry = '';

select *
from layoffs_staging2
where company = 'Airbnb';

select t1.industry, t2.industry
from layoffs_staging2 as t1
join layoffs_staging2 as t2
	on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry is NULL or t1.industry = '')
and t2.industry is NOT NULL
;

-- b. join the NULL column with the NOT NULL column where the column has the same company name
update layoffs_staging2 as t1
join layoffs_staging2 as t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is NULL
and t2.industry is NOT NULL
; 

select *
from layoffs_staging2
where company like 'Bally%';

-- c. Delete all the data where the 'total_laid_off' and 'percentage_laid_off' column is NULL
delete
from layoffs_staging2
where total_laid_off is NULL
and percentage_laid_off is NULL
;

select *
from layoffs_staging2;

-- 4. drop the 'row_num' column
alter table layoffs_staging2
drop column row_num;

