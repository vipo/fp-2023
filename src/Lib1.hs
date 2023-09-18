{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    validateDataFrame,
    renderDataFrameAsTable,
  )
where

import Data.List (intercalate)
import DataFrame (DataFrame(..), Column(..), Value(..), ColumnType(..))
import InMemoryTables (TableName)
import Data.Char (toLower)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

-- 1) Return a data frame by its name from the provided Database list
findTableByName :: Database -> String -> Maybe DataFrame
findTableByName db tableName = lookup (map toLower tableName) (map (\(name, df) -> (map toLower name, df)) db)

-- 2) Parse a "select * from ..." SQL statement and extract the table name
parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement stmt = 
  case words stmt of
    ("select" : "*" : "from" : tableName : _) -> Right tableName
    _ -> Left "Invalid SQL Statement"

-- 3) Validate tables: check if columns match value types, if row sizes match columns, etc.
validateDataFrame :: DataFrame -> Either ErrorMessage ()
validateDataFrame (DataFrame columns rows) 
  | null rows = Right ()
  | otherwise = 
      let expectedNumColumns = length columns
          validateRow r = if length r == expectedNumColumns then Right () else Left "Row size doesn't match columns"
          validateRows = mapM_ validateRow rows
          validateTypes = all validateType (zip columns (transpose rows))
      in if not validateTypes then Left "Type mismatch in columns" else validateRows

validateType :: (Column, [Value]) -> Bool
validateType (Column _ colType, values) = all (matchesType colType) values

matchesType :: ColumnType -> Value -> Bool
matchesType IntegerType (IntegerValue _) = True
matchesType StringType (StringValue _) = True
matchesType BoolType (BoolValue _) = True
matchesType _ NullValue = True
matchesType _ _ = False

-- 4) Render a given data frame as an ASCII-art table
renderDataFrameAsTable :: Integer -> DataFrame -> String
renderDataFrameAsTable terminalWidth (DataFrame columns rows) = 
    let header = map (\(Column name _) -> name) columns
        renderedRows = map (map renderValue) rows
        allRows = header : renderedRows
        colWidths = map (\col -> maximum $ map length col) (transpose allRows)
        totalWidth = sum colWidths + length colWidths + 1
        truncation = if totalWidth > fromIntegral terminalWidth then "..." else ""
        renderRow row = "|" ++ intercalate "|" (zipWith pad colWidths row) ++ "|"
        separator = "+" ++ intercalate "+" (map (\w -> replicate w '-') colWidths) ++ "+"
    in unlines $ [renderRow header, separator] ++ map renderRow renderedRows ++ [truncation]

pad :: Int -> String -> String
pad width str = str ++ replicate (width - length str) ' '

renderValue :: Value -> String
renderValue (IntegerValue i) = show i
renderValue (StringValue s) = s
renderValue (BoolValue b) = show b
renderValue NullValue = "NULL"

transpose :: [[a]] -> [[a]]
transpose ([]:_) = []
transpose x = (map head x) : transpose (map tail x)
