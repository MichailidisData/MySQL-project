SELECT *
FROM covid_19;


-- Create a copy table. This is the one we will work in and clean the data. 

CREATE TABLE covid19.covid_19_copy 
LIKE covid19.covid_19;

INSERT INTO covid19.covid_19_copy
SELECT * FROM covid19.covid_19;

SELECT *
FROM covid19.covid_19_copy;


-- check for duplicates

SELECT *, 
	ROW_NUMBER() OVER (
		PARTITION BY `Date_reported`, `Country_code`, `Country`, `WHO_region`, `New_cases`, 
			`Cumulative_cases`, `New_deaths`, `Cumulative_deaths`) AS row_num
FROM covid19.covid_19_copy;  


SELECT *
FROM (
	SELECT *, ROW_NUMBER() OVER (
				PARTITION BY `Date_reported`, `Country_code`, `Country`, `WHO_region`, `New_cases`, 
				`Cumulative_cases`, `New_deaths`, `Cumulative_deaths`) AS row_num
	FROM covid19.covid_19_copy 
    ) AS duplicates
WHERE row_num > 1 ;

-- we do not have duplicates based on previous result


-- replace all NULL or BLANKS with 0

SELECT * 
FROM covid19.covid_19_copy
WHERE `New_cases` LIKE '' OR `New_deaths` LIKE '';


UPDATE covid19.covid_19_copy 
SET `New_cases` = 0
WHERE `New_cases` LIKE '';

UPDATE covid19.covid_19_copy 
SET `New_deaths` = 0
WHERE `New_deaths` LIKE '';


-- remove columns that we do not need

ALTER TABLE covid19.covid_19_copy
DROP COLUMN `Country_code`;

ALTER TABLE covid19.covid_19_copy
DROP COLUMN `WHO_region`;

SELECT *
FROM covid19.covid_19_copy;


-- 2020 death per case percentage

SELECT Country, 
	MAX(Cumulative_cases) cases_2020, 
	MAX(Cumulative_deaths) deaths_2020,
    CASE
	WHEN MAX(Cumulative_cases) = 0 THEN 0
        WHEN MAX(Cumulative_cases) != 0 THEN (MAX(Cumulative_deaths) / MAX(Cumulative_cases)) * 100
	END AS death_per_case_2020
FROM covid19.covid_19_copy
WHERE `Date_reported` LIKE '2020%' 	
GROUP BY Country
ORDER BY death_per_case_2020 DESC; 


-- 2021 death per case percentage

SELECT Country, 
	MAX(Cumulative_cases) cases_2021, 
	MAX(Cumulative_deaths) deaths_2021,
    CASE
	WHEN MAX(Cumulative_cases) = 0 THEN 0
        WHEN MAX(Cumulative_cases) != 0 THEN (MAX(Cumulative_deaths) / MAX(Cumulative_cases)) * 100
	END AS death_per_case_2021
FROM covid19.covid_19_copy
WHERE `Date_reported` LIKE '2021%' 	
GROUP BY Country
ORDER BY death_per_case_2021 DESC; 


-- 2022 death per case percentage

SELECT Country, 
	MAX(Cumulative_cases) cases_2022, 
	MAX(Cumulative_deaths) deaths_2022,
    CASE
	WHEN MAX(Cumulative_cases) = 0 THEN 0
        WHEN MAX(Cumulative_cases) != 0 THEN (MAX(Cumulative_deaths) / MAX(Cumulative_cases)) * 100
	END AS death_per_case_2022
FROM covid19.covid_19_copy
WHERE `Date_reported` LIKE '2022%' 	
GROUP BY Country
ORDER BY death_per_case_2022 DESC; 



-- 2023 death per case percentage

SELECT Country, 
	MAX(Cumulative_cases) cases_2023, 
	MAX(Cumulative_deaths) deaths_2023,
    CASE
	WHEN MAX(Cumulative_cases) = 0 THEN 0
        WHEN MAX(Cumulative_cases) != 0 THEN (MAX(Cumulative_deaths) / MAX(Cumulative_cases)) * 100
	END AS death_per_case_2023
FROM covid19.covid_19_copy
WHERE `Date_reported` LIKE '2023%' 	
GROUP BY Country
ORDER BY death_per_case_2023 DESC; 



-- 2024 death per case percentage

SELECT Country, 
	MAX(Cumulative_cases) cases_2024, 
	MAX(Cumulative_deaths) deaths_2024,
    CASE
	WHEN MAX(Cumulative_cases) = 0 THEN 0
        WHEN MAX(Cumulative_cases) != 0 THEN (MAX(Cumulative_deaths) / MAX(Cumulative_cases)) * 100
	END AS death_per_case_2024
FROM covid19.covid_19_copy
WHERE `Date_reported` LIKE '2024%' 	
GROUP BY Country
ORDER BY death_per_case_2024 DESC; 



-- increase in deaths per year (2020 - 2021)

WITH covid_2020_cte AS
    (
    SELECT Country, 
    	MAX(Cumulative_deaths) deaths_2020
    FROM covid19.covid_19_copy
    WHERE `Date_reported` LIKE '2020%' 	
    GROUP BY Country
    ),
    covid_2021_cte AS
    (
    SELECT Country, 
	MAX(Cumulative_deaths) deaths_2021
     FROM covid19.covid_19_copy
     WHERE `Date_reported` LIKE '2021%' 	
     GROUP BY Country
    )
    SELECT c_20.Country, c_20.deaths_2020, c_21.deaths_2021,
		CASE 
		     WHEN deaths_2020 = 0 THEN deaths_2021 / 1
                     WHEN deaths_2020 != 0 THEN deaths_2021 / deaths_2020
		END AS death_increase_percentage
    FROM covid_2020_cte c_20
    JOIN covid_2021_cte c_21
    ON c_20.Country = c_21.Country
    ORDER BY death_increase_percentage DESC;
    


-- CTE increase in deaths per year (20 - 21), (21 - 22), (22 - 23), (23 - 24) 
WITH covid_2020_cte AS
    (
    SELECT Country, 
	MAX(Cumulative_deaths) deaths_2020
    FROM covid19.covid_19_copy
    WHERE `Date_reported` LIKE '2020%' 	
    GROUP BY Country
    ),
    covid_2021_cte AS
    (
    SELECT Country, 
	MAX(Cumulative_deaths) deaths_2021
    FROM covid19.covid_19_copy
    WHERE `Date_reported` LIKE '2021%' 	
    GROUP BY Country
    ),
    covid_2022_cte AS
    (
    SELECT Country, 
	MAX(Cumulative_deaths) deaths_2022
    FROM covid19.covid_19_copy
    WHERE `Date_reported` LIKE '2022%' 	
    GROUP BY Country
    ),
    covid_2023_cte AS
    (
    SELECT Country, 
	MAX(Cumulative_deaths) deaths_2023
    FROM covid19.covid_19_copy
    WHERE `Date_reported` LIKE '2023%' 	
    GROUP BY Country
    ),
    covid_2024_cte AS
    (
    SELECT Country, 
	MAX(Cumulative_deaths) deaths_2024
    FROM covid19.covid_19_copy
    WHERE `Date_reported` LIKE '2024%' 	
    GROUP BY Country 
    )
    SELECT c_20.Country,
	CASE 
	    WHEN deaths_2020 = 0 THEN deaths_2021 / 1
            WHEN deaths_2020 != 0 THEN deaths_2021 / deaths_2020
	END AS death_percentage_20_21,
        CASE 
	    WHEN deaths_2021 = 0 THEN deaths_2022 / 1
            WHEN deaths_2021 != 0 THEN deaths_2022 / deaths_2021
	END AS death_percentage_21_22,
        CASE 
	    WHEN deaths_2022 = 0 THEN deaths_2023 / 1
            WHEN deaths_2022 != 0 THEN deaths_2023 / deaths_2022
	END AS death_percentage_22_23,
        CASE 
	    WHEN deaths_2023 = 0 THEN deaths_2024 / 1
            WHEN deaths_2023 != 0 THEN deaths_2024 / deaths_2023
	END AS death_percentage_23_24
    FROM covid_2020_cte c_20
    JOIN covid_2021_cte c_21
    ON c_20.Country = c_21.Country
    JOIN covid_2022_cte c_22
    ON c_21.Country = c_22.Country
    JOIN covid_2023_cte c_23
    ON c_22.Country = c_23.Country
    JOIN covid_2024_cte c_24
    ON c_23.Country = c_24.Country
    ORDER BY c_20.Country;
    
