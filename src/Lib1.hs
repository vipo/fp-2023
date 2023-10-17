{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    validateDataFrame,
    renderDataFrameAsTable,
  )
where

import Data.List (intercalate, findIndex)
import DataFrame (DataFrame(..), Column(..), Value(..), ColumnType(..), Row)
import InMemoryTables (TableName)
import Data.Char (toLower, toUpper)
import Data.Maybe (fromJust, isJust)
import Data.Foldable (asum)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

-- 1) Return a data frame by its name from the provided Database list
findTableByName :: Database -> String -> Maybe DataFrame
findTableByName db tableName = 
    lookup (map toLower tableName) (map (\(name, df) -> (map toLower name, df)) db)

-- 2) Implement the function which parses a "select * from ..."
-- sql statement and extracts a table name from the statement
parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement "" = Left "Empty select statement"
parseSelectAllStatement statement =
  case words [toUpper x | x <- statement] of
    ["SELECT", "*", "FROM", tableName] -> validTableName tableName
    _ -> Left "Invalid SQL Statement"
  where
    originalTableName = last (words statement)
    
    validTableName tableNameToCheck
      | last tableNameToCheck == ';' && length tableNameToCheck > 1 = Right (removeLastChar originalTableName)
      | otherwise = Left "Invalid sql statement"
    
    removeLastChar :: String -> String
    removeLastChar [a] = []
    removeLastChar (x : xs) = x : removeLastChar xs

-- 3) Validate tables: check if columns match value types, if row sizes match columns, etc.
validateDataFrame :: DataFrame -> Either ErrorMessage ()
validateDataFrame (DataFrame [] _) = Left "DataFrame has no columns"    
validateDataFrame (DataFrame _ []) = Left "DataFrame has no rows"
validateDataFrame (DataFrame cols rows) = 
         case findMismatchedRow rows of
          Just rowIndex -> Left $ "Row " ++ show rowIndex ++ " size doesn't match columns"
          Nothing -> 
              case findMismatchedColumnType rows of
                  Just (rowIndex, colIndex) -> Left $ "Type mismatch in row " ++ show rowIndex ++ ", column " ++ show colIndex
                  Nothing -> Right ()
      where
        findMismatchedRow :: [Row] -> Maybe Int
        findMismatchedRow = findIndex (\row -> length row /= length cols)
    
        findMismatchedColumnType :: [Row] -> Maybe (Int, Int)
        findMismatchedColumnType = findIndexWithIndex (\rowIndex row -> 
            isJust (findIndex (not . uncurry validateColumnValue) (zip cols row)))
    
        validateColumnValue :: Column -> Value -> Bool
        validateColumnValue (Column _ IntegerType) (IntegerValue _) = True
        validateColumnValue (Column _ StringType) (StringValue _) = True
        validateColumnValue (Column _ BoolType) (BoolValue _) = True
        validateColumnValue (Column _ _) NullValue = True  -- Assuming Null is allowed for all types
        validateColumnValue _ _ = False
    
        findIndexWithIndex :: (Int -> a -> Bool) -> [a] -> Maybe (Int, Int)
        findIndexWithIndex p xs = asum $ zipWith (\i x -> if p i x then Just (i, fromJust (findIndex (p i) xs)) else Nothing) [0..] xs
    
-- 4) Render a given data frame as an ASCII-art table
renderDataFrameAsTable :: Integer -> DataFrame -> String
renderDataFrameAsTable terminalWidth (DataFrame columns rows) = 
    let header = map (\(Column name _) -> name) columns
        renderedRows = map (map renderValue) rows
        allRows = header : renderedRows
        colWidths = map (\col -> maximum $ map length col) (myTranspose allRows)
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

myTranspose :: [[a]] -> [[a]]
myTranspose ([]:_) = []
myTranspose x = (map head x) : myTranspose (map tail x)