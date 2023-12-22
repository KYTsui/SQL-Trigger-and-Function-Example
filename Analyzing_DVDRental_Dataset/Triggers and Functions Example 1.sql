-- Creating the detailed and summary tables with their respective columns. 
CREATE TABLE summary_table (
    store_weekday_id VARCHAR(20) NOT NULL,
    rental_weekday VARCHAR(20),
    rental_count INT,
    address VARCHAR(50),
    PRIMARY KEY (store_weekday_id)
);

CREATE TABLE detailed_table (
    rental_id INT NOT NULL,
    rental_date DATE,
    rental_weekday VARCHAR(20),
	store_weekday_id VARCHAR(20),
    staff_id INT,
    store_id INT,
    address VARCHAR(50),
    PRIMARY KEY (rental_id),
    FOREIGN KEY (store_weekday_id) REFERENCES summary_table(store_weekday_id)
);

-- Extracting raw data for the detailed table from the rental table.
INSERT INTO detailed_table (rental_id, rental_date, staff_id, rental_weekday)
SELECT rental.rental_id, rental.rental_date, rental.staff_id, To_Char(rental.rental_date, 'Day')
FROM rental;

--Create a view and join with the staff table.
CREATE VIEW info_view
AS
SELECT detailed_table.rental_id, detailed_table.staff_id, staff.store_id, store.address_id, address.address
FROM detailed_table
JOIN staff
ON detailed_table.staff_id = staff.staff_id
JOIN store
On staff.staff_id = store.manager_staff_id
JOIN address
ON store.address_id = address.address_id 

--Insert store_id and address into detailed table from the newly created view.
UPDATE detailed_table
SET store_id = info_view.store_id, address = info_view.address 
FROM info_view
WHERE detailed_table.rental_id = info_view.rental_id;

--Lastly, fill in the store_weekday_id.
INSERT INTO summary_table(store_weekday_id)
SELECT DISTINCT CONCAT(detailed_table.store_id, detailed_table.rental_weekday)
FROM detailed_table;
UPDATE detailed_table
SET store_weekday_id = CONCAT(detailed_table.store_id, detailed_table.rental_weekday);


-- Creating a function
CREATE FUNCTION populate()
    RETURNS TRIGGER
    LANGUAGE PLPGSQL
    AS
$$
BEGIN
	UPDATE summary_table
	SET rental_count = subquery.count_rental_weekday
	FROM (
  	  SELECT summary_table.store_weekday_id, COUNT(detailed_table.rental_weekday) AS count_rental_weekday
  	  FROM summary_table
  	  JOIN detailed_table
      ON summary_table.store_weekday_id = detailed_table.store_weekday_id
 	  GROUP BY summary_table.store_weekday_id, detailed_table.rental_weekday     	
) AS subquery
WHERE summary_table.store_weekday_id = subquery.store_weekday_id;

	UPDATE summary_table
	SET rental_weekday = subquery.rental_weekday, address = subquery.address
	FROM (
    SELECT DISTINCT ON (detailed_table.store_weekday_id)
        detailed_table.store_weekday_id,
        detailed_table.rental_weekday,
        detailed_table.address
    FROM detailed_table
) AS subquery
WHERE summary_table.store_weekday_id = subquery.store_weekday_id;
RETURN NEW;
END;
$$

-- Create a trigger after creating the function.
CREATE TRIGGER calculate 
    AFTER UPDATE
    ON detailed_table
    FOR EACH STATEMENT
    EXECUTE PROCEDURE populate()


-- Create stored procedure.
CREATE PROCEDURE refresh_tables()
LANGUAGE PLPGSQL
AS $$
--Here, we declare the latest_date variable that we will use later. 
DECLARE
    last_date date;
BEGIN
   --First, clear out content from detailed table. 
   TRUNCATE TABLE detailed_table CASCADE;
   TRUNCATE TABLE summary_table CASCADE;

    --To extract raw data for the detailed table from the rental table.
   INSERT INTO detailed_table (rental_id, rental_date, staff_id, rental_weekday)
   SELECT rental.rental_id, rental.rental_date, rental.staff_id, To_Char(rental.rental_date, 'Day')
   FROM rental;

   --Create a view and join with the staff table.
   CREATE VIEW info_view2
   AS
   SELECT detailed_table.rental_id, detailed_table.staff_id, staff.store_id, store.address_id, address.address
   FROM detailed_table
   JOIN staff
   ON detailed_table.staff_id = staff.staff_id
   JOIN store
   On staff.staff_id = store.manager_staff_id
   JOIN address
   ON store.address_id = address.address_id;

   --Insert store_id and address into the detailed table from the newly created view.
   UPDATE detailed_table
   SET store_id = info_view2.store_id, address = info_view2.address 
   FROM info_view2
   WHERE detailed_table.rental_id = info_view2.rental_id;

   --Lastly, fill in the store_weekday_id in the summary and detailed table.
   INSERT INTO summary_table(store_weekday_id)
   SELECT DISTINCT CONCAT(detailed_table.store_id,  detailed_table.rental_weekday)
   FROM detailed_table;
   UPDATE detailed_table
   SET store_weekday_id = CONCAT(store_id, rental_weekday);
   
   -- Now we need to find the latest record in the rental_date column.
   SELECT MAX(rental_date) INTO last_date FROM detailed_table;
	
   -- Delete rows that are 10 months older than the latest record.
   DELETE FROM detailed_table
   WHERE rental_date < (last_date - INTERVAL '10 months');

   --Remeber to drop the view so that new view is always created on the fly.
   DROP VIEW info_view2;
  
END;
$$;


--Test if it really works.
CALL refresh_tables();

SELECT * FROM detailed_table;

SELECT * FROM summary_table 
ORDER BY address, rental_count DESC;



-------------------------------------------------------------------------------------------
-- To delete the created procedure, trigger, function, and tables, tun the following lines.
DROP PROCEDURE refresh_tables;
DROP trigger calculate on detailed_table;
DROP function populate;
DROP VIEW info_view;
DROP table summary_table CASCADE;
DROP table detailed_table CASCADE;
