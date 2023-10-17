{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement(..)
  )
where

import Data.Char (toLower)
import DataFrame (DataFrame(..), Column(..), ColumnType(..), Value(..))
import InMemoryTables (TableName, database)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

-- Keep the type, modify constructors
--data ParsedStatement = ParsedStatement

data ParsedStatement 
  = ShowTable TableName
  deriving (Show, Eq)
  
-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input = 
  case words (map toLower input) of
    ["show", "table", tableName] 
      | last tableName == ';' -> Right $ ShowTable (init tableName)  -- Remove the last character (semicolon)
      | otherwise -> Left "Unsupported or invalid statement"
    _ -> Left "Unsupported or invalid statement"




-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement (ShowTable tableName) = 
  case lookup tableName database of
    Just df -> Right $ DataFrame [Column "Columns" StringType] (map (\(Column name _) -> [StringValue name]) (columns df))
    Nothing -> Left $ "Table " ++ tableName ++ " not found"

-- Extract the columns of a DataFrame
columns :: DataFrame -> [Column]
columns (DataFrame cols _) = cols
