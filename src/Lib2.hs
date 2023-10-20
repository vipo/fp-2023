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
    = ShowTableStmt TableName
    | ShowAllTablesStmt
    deriving (Show, Eq)

parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input 
  | last input /= ';' = Left "Unsupported or invalid statement"
  | otherwise = 
      let cleanedInput = init input
      in case words (map toLower cleanedInput) of
          ["show", "table", tableName] -> Right $ ShowTableStmt tableName
          ["show", "tables"] -> Right ShowAllTablesStmt
          _ -> Left "Unsupported or invalid statement"

executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement (ShowTableStmt tableName) = 
  case lookup tableName database of
    Just df -> Right $ DataFrame [Column "Columns" StringType] (map (\(Column name _) -> [StringValue name]) (columns df))
    Nothing -> Left $ "Table " ++ tableName ++ " not found"
executeStatement ShowAllTablesStmt =
  Right $ DataFrame [Column "Tables" StringType] 
                   (map (\(name, _) -> [StringValue name]) database) 

columns :: DataFrame -> [Column]
columns (DataFrame cols _) = cols