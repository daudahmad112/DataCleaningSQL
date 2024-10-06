CREATE DATABASE world_layoffs;
USE world_layoffs;

SELECT * FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null values
-- 4. Remove any columns if required

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT * FROM layoffs;

SELECT * FROM layoffs_staging;


-- 1. delete duplicate records

SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS (
	SELECT *, 
	ROW_NUMBER() OVER(
	PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country,
    funds_raised_millions) AS row_num
	FROM layoffs_staging
)
SELECT * FROM duplicate_cte
WHERE row_num > 1;

SELECT * FROM layoffs_staging
WHERE company = 'Casper';


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

SELECT * FROM layoffs_staging2;

INSERT INTO layoffs_staging2
	SELECT *, 
	ROW_NUMBER() OVER(
	PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country,
    funds_raised_millions) AS row_num
	FROM layoffs_staging;


SELECT * FROM layoffs_staging2
WHERE row_num>1;

DELETE FROM layoffs_staging2
WHERE row_num>1;

-- 2. standardize data


SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT company FROM layoffs_staging2;

--
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';


UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

--

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR industry ='';

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;


UPDATE layoffs_staging2
SET industry = 'Lodging'
WHERE company = 'Airbnb';

SELECT * FROM layoffs_staging2
WHERE company = 'Airbnb';


SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

--

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;


UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';

--

SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

SELECT `date` FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

--

SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

--

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry ='';


UPDATE layoffs_staging2
SET 
industry = NULL
WHERE
industry = '';

UPDATE layoffs_staging2 t1
JOIN 
	layoffs_staging2 t2
ON 
t1.company = t2.company
AND 
t1.location = t2.location
SET 
t1.industry = t2.industry
WHERE
(t1.industry IS NULL)
AND
(t2.industry IS NOT NULL);

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


-- EXPLORATORY DATA ANALYSIS --
-- ----- BEGIN HERE ---------

SELECT MAX(total_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off=1
ORDER BY total_laid_off DESC;

SELECT *
FROM layoffs_staging2
ORDER BY total_laid_off DESC
LIMIT 5;

SELECT company, SUM(total_laid_off), total_laid_off
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT company, SUM(total_laid_off), MIN(`date`), MAX(`date`)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT industry, SUM(total_laid_off), MIN(`date`), MAX(`date`)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off), MIN(`date`), MAX(`date`)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 2 DESC;

SELECT stage, SUM(total_laid_off), MIN(`date`), MAX(`date`)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;


SELECT stage, SUM(total_laid_off), MIN(`date`), MAX(`date`)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;


-- rolling total layoffs

SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1;


WITH Rolling_Total AS (
	SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) AS sum_laid_off
	FROM layoffs_staging2
	WHERE SUBSTRING(`date`,1,7) IS NOT NULL
	GROUP BY `MONTH`
	ORDER BY 1
)
SELECT `MONTH`, sum_laid_off, SUM(sum_laid_off) OVER(ORDER BY `Month`) AS rolling_total_laid_off
FROM Rolling_Total;

SELECT company, YEAR(`date`) , SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

WITH Company_Year AS
(
	SELECT company, YEAR(`date`) as 'years' , SUM(total_laid_off) as 'total_laid_off'
	FROM layoffs_staging2
	GROUP BY company, YEAR(`date`)
), 
Company_YEAR_Rank AS
	(SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
	FROM Company_Year
	WHERE years IS NOT NULL)
SELECT * FROM Company_YEAR_Rank
WHERE ranking <=5;






















