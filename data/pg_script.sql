-- PostgreSQL Setup Script: Schema, Admin, and Stored Procedures

-- 1. Role and Database Setup (DB Admin Script)
-- ----------------------------------------------
-- Create a dedicated database and user for the application
CREATE DATABASE recipe_app;

-- Connect to the database (run in psql or via your tool of choice)
\c recipe_app;

-- Create application user and admin user
CREATE ROLE app_user WITH LOGIN PASSWORD 'app_user_password';
CREATE ROLE app_admin WITH LOGIN PASSWORD 'app_admin_password';

-- Grant privileges
GRANT CONNECT ON DATABASE recipe_app TO app_user, app_admin;
GRANT ALL PRIVILEGES ON DATABASE recipe_app TO app_admin;

-- Schema creation for recipes and measurements
-- --------------------------------------------

CREATE SCHEMA IF NOT EXISTS recipes;

-- 2. Tables
-- ----------

-- recipes.recipe_definitions
CREATE TABLE recipes.recipe_definitions (
    recipe_id        TEXT       NOT NULL,
    version          TEXT       NOT NULL,
    test_number      INT        NOT NULL,
    test_name        TEXT       NOT NULL,
    characteristic   TEXT       NOT NULL,
    target_value     DOUBLE PRECISION NOT NULL,
    tol_lower        DOUBLE PRECISION NOT NULL,
    tol_upper        DOUBLE PRECISION NOT NULL,
    unit             TEXT       NOT NULL,
    method_reference TEXT       NOT NULL,
    remarks          TEXT,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (recipe_id, version, test_number)
);


-- 3. Grant table-specific privileges
-- ----------------------------------
GRANT USAGE ON SCHEMA recipes TO app_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA recipes TO app_user;

-- Allow app_admin full rights
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA recipes TO app_admin;

-- 4. Stored Procedures for Webservice and Website
-- ------------------------------------------------

-- 4.1 Insert or update recipe definition
CREATE OR REPLACE FUNCTION recipes.upsert_recipe(
    p_recipe_id TEXT,
    p_version TEXT,
    p_test_number INT,
    p_test_name TEXT,
    p_characteristic TEXT,
    p_target_value DOUBLE PRECISION,
    p_tol_lower DOUBLE PRECISION,
    p_tol_upper DOUBLE PRECISION,
    p_unit TEXT,
    p_method_reference TEXT,
    p_remarks TEXT
) RETURNS VOID AS $$
BEGIN
    INSERT INTO recipes.recipe_definitions(
        recipe_id, version, test_number, test_name, characteristic,
        target_value, tol_lower, tol_upper, unit, method_reference, remarks)
    VALUES(
        p_recipe_id, p_version, p_test_number, p_test_name, p_characteristic,
        p_target_value, p_tol_lower, p_tol_upper, p_unit, p_method_reference, p_remarks)
    ON CONFLICT (recipe_id, version, test_number) DO UPDATE
    SET test_name = EXCLUDED.test_name,
        characteristic = EXCLUDED.characteristic,
        target_value = EXCLUDED.target_value,
        tol_lower = EXCLUDED.tol_lower,
        tol_upper = EXCLUDED.tol_upper,
        unit = EXCLUDED.unit,
        method_reference = EXCLUDED.method_reference,
        remarks = EXCLUDED.remarks;
END;
$$ LANGUAGE plpgsql;

-- End of Setup Script


-- 4.2 Retrieve all recipes and versions
CREATE OR REPLACE FUNCTION recipes.get_all_recipes()
RETURNS TABLE(
    recipe_id TEXT,
    version TEXT,
    test_number INT,
    test_name TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT recipe_id, version, test_number, test_name
      FROM recipes.recipe_definitions
      ORDER BY recipe_id, version, test_number;
END;
$$ LANGUAGE plpgsql;

-- 4.3 Retrieve definitions for a specific recipe and version
CREATE OR REPLACE FUNCTION recipes.get_recipe_version(
    p_recipe_id TEXT,
    p_version TEXT
) RETURNS TABLE(
    test_number INT,
    test_name TEXT,
    characteristic TEXT,
    target_value DOUBLE PRECISION,
    tol_lower DOUBLE PRECISION,
    tol_upper DOUBLE PRECISION,
    unit TEXT,
    method_reference TEXT,
    remarks TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT test_number, test_name, characteristic,
           target_value, tol_lower, tol_upper,
           unit, method_reference, remarks
      FROM recipes.recipe_definitions
     WHERE recipe_id = p_recipe_id
       AND version = p_version
     ORDER BY test_number;
END;
$$ LANGUAGE plpgsql;

-- 4.4 Delete a specific recipe version
CREATE OR REPLACE FUNCTION recipes.delete_recipe_version(
    p_recipe_id TEXT,
    p_version TEXT
) RETURNS VOID AS $$
BEGIN
    DELETE FROM recipes.recipe_definitions
     WHERE recipe_id = p_recipe_id
       AND version = p_version;
END;
$$ LANGUAGE plpgsql;

-- End of Recipes Schema and Procedures


-- 4.5 Get latest version for a given recipe_id
CREATE OR REPLACE FUNCTION recipes.get_latest_version(
    p_recipe_id TEXT
) RETURNS TEXT AS $$
DECLARE
    latest_ver TEXT;
BEGIN
    SELECT version
      INTO latest_ver
      FROM recipes.recipe_definitions
     WHERE recipe_id = p_recipe_id
     GROUP BY version
     ORDER BY string_to_array(version, '.')::int[] DESC
     LIMIT 1;
    RETURN latest_ver;
END;
$$ LANGUAGE plpgsql;

-- 4.6 Get recipe info by test number for a given recipe and version
CREATE OR REPLACE FUNCTION recipes.get_test_info(
    p_recipe_id TEXT,
    p_version TEXT,
    p_test_number INT
) RETURNS TABLE(
    test_name TEXT,
    characteristic TEXT,
    target_value DOUBLE PRECISION,
    tol_lower DOUBLE PRECISION,
    tol_upper DOUBLE PRECISION,
    unit TEXT,
    method_reference TEXT,
    remarks TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT test_name, characteristic,
           target_value, tol_lower, tol_upper,
           unit, method_reference, remarks
      FROM recipes.recipe_definitions
     WHERE recipe_id = p_recipe_id
       AND version = p_version
       AND test_number = p_test_number;
END;
$$ LANGUAGE plpgsql;

-- 4.7 Extract recipe_id and version from filename, then get latest version
CREATE OR REPLACE FUNCTION recipes.get_latest_version_from_filename(
    p_filename TEXT
) RETURNS TEXT AS $$
DECLARE
    rid TEXT;
    ver TEXT;
    base TEXT;
BEGIN
    -- Strip extension
    base := substring(p_filename from '^(.*)\\.csv$');
    -- Split on underscore
    rid := split_part(base, '_', 1);
    -- Return computed latest version
    RETURN recipes.get_latest_version(rid);
END;
$$ LANGUAGE plpgsql;

-- 4.8 Get test info from filename and test number (uses latest version)
CREATE OR REPLACE FUNCTION recipes.get_test_info_from_filename(
    p_filename TEXT,
    p_test_number INT
) RETURNS TABLE(
    recipe_id TEXT,
    version TEXT,
    test_number INT,
    test_name TEXT,
    characteristic TEXT,
    target_value DOUBLE PRECISION,
    tol_lower DOUBLE PRECISION,
    tol_upper DOUBLE PRECISION,
    unit TEXT,
    method_reference TEXT,
    remarks TEXT
) AS $$
DECLARE
    rid TEXT;
    latest_ver TEXT;
BEGIN
    rid := split_part(substring(p_filename from '^(.*)\\.csv$'), '_', 1);
    latest_ver := recipes.get_latest_version(rid);
    RETURN QUERY
    SELECT recipe_id, version, test_number,
           test_name, characteristic,
           target_value, tol_lower, tol_upper,
           unit, method_reference, remarks
      FROM recipes.recipe_definitions
     WHERE recipe_id = rid
       AND version = latest_ver
       AND test_number = p_test_number;
END;
$$ LANGUAGE plpgsql;

-- End of Recipes Schema and Advanced Procedures
