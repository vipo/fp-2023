{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement
  )
where

import DataFrame (Column (..), ColumnType (..), Value (..), DataFrame (..), Row (..))
import InMemoryTables (TableName, database)
import Lib1 (findTableByName)
import Data.Char (toLower)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

data Operator = And [Operator]
              | Equal String Value
              | Less String Value
              | Greater String Value
              | LessThanEqual String Value
              | GreaterThanEqual String Value

-- Keep the type, modify constructors
data ParsedStatement
  = ShowTables
  | ShowTable TableName
  | Select [String] TableName (Maybe Operator)
  | ParsedStatement
  | Min String TableName String
  | Sum String TableName String

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input
  | null input = Left "Empty input"
  | last input == ';' = parseStatement (init input)
  | otherwise =
    case words (map toLower input) of
      ["show", "tables"] -> Right ShowTables
      ["show", "table", table] -> Right (ShowTable table)
      _ -> Left "Invalid SELECT statement"

-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement ShowTables = Right $ DataFrame [Column "table_name" StringType] (map (\(name, _) -> [StringValue name]) database)
executeStatement (ShowTable tablename) =
  case lookup (map toLower tablename) database of
    Just a -> Right $ DataFrame [Column "column_names" StringType] (map (\col -> [StringValue (columnName col)]) (columns a))
    Nothing -> Left "Table not found"
executeStatement _ = Left "Not implemented"

columnName :: Column -> String
columnName (Column name _) = name

columnNames :: [Column] -> [String]
columnNames = map (\(Column name _) -> name)

columns :: DataFrame -> [Column]
columns (DataFrame cols _) = cols