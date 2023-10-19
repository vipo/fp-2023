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

data ParsedStatement 
  = ShowTable TableName
  | ShowTables
  deriving (Show, Eq)
  
-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input
  | last input /= ';' = Left "Unsupported or invalid statement"
  | otherwise = 
      let cleanedInput = init input
      in case words (map toLower cleanedInput) of
          ["show", "table", tableName] -> Right $ ShowTable tableName
          ["show", "tables"] -> Right ShowTables
          _ -> Left "Unsupported or invalid statement"

-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement (ShowTable tableName) = 
  case lookup tableName database of
    Just df -> Right $ DataFrame [Column "Columns" StringType] (map (\(Column name _) -> [StringValue name]) (columns df))
    Nothing -> Left $ "Table " ++ tableName ++ " not found"
executeStatement ShowTables =
  Right $ DataFrame [Column "Tables" StringType] 
                   (map (\(name, _) -> [StringValue name]) database)  -- Produce a DataFrame listing all table names

-- Extract the columns of a DataFrame
columns :: DataFrame -> [Column]
columns (DataFrame cols _) = cols
