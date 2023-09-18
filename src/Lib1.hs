{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    validateDataFrame,
    renderDataFrameAsTable,
  )
where

import DataFrame (DataFrame (..), Row, Column (..), ColumnType (..), Value (..))
import InMemoryTables (TableName)

type ErrorMessage = String

type Database = [(TableName, DataFrame)]

-- Your code modifications go below this comment

-- 1) implement the function which returns a data frame by its name
-- in provided Database list
findTableByName :: Database -> String -> Maybe DataFrame
findTableByName database tableName = findTableByName' database (lowerCaseString tableName)


findTableByName' :: Database -> String -> Maybe DataFrame
findTableByName' [] _ = Nothing
findTableByName' (x:xs) lowerTableName
  | lowerCaseString (fst x) == lowerTableName = (Just (snd x))
  | otherwise = findTableByName' xs lowerTableName


lowerCaseString :: String -> String
lowerCaseString [] = []
lowerCaseString (x:xs)
  | isLowerCase x == True = toEnum (fromEnum x + 32) : lowerCaseString xs
  | otherwise = x : lowerCaseString xs


isLowerCase :: Char -> Bool
isLowerCase c = if c >= 'A' && c <= 'Z' then True else False 

--findTableByName _ _ = error "findTableByName not implemented"

-- 2) implement the function which parses a "select * from ..."
-- sql statement and extracts a table name from the statement
parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement _ = error "parseSelectAllStatement not implemented"

-- 3) implement the function which validates tables: checks if
-- columns match value types, if rows sizes match columns,..
validateDataFrame :: DataFrame -> Either ErrorMessage ()
validateDataFrame (DataFrame cols rows) = validateRows rows
    where
        validateRows :: [Row] -> Either ErrorMessage ()
        validateRows [] = Right ()
        validateRows (r:rs) = 
            case validateRow cols r of
                Left err -> Left err
                Right _ -> validateRows rs

        validateRow :: [Column] -> [Value] -> Either ErrorMessage ()
        validateRow [] [] = Right ()
        validateRow (_:_) [] = Left "Row does not have enough values."
        validateRow [] (_:_) = Left "Row has too many values."
        validateRow (c:cs) (v:vs)
            | doesValueTypeMatchColType c v = validateRow cs vs
            | otherwise = Left "Cell's value type does not match the column's type."

        doesValueTypeMatchColType :: Column -> Value -> Bool
        doesValueTypeMatchColType _ NullValue = True
        doesValueTypeMatchColType col val =
            case getColTypeByValue val of
                Left _ -> False
                Right colType -> colType == getColType col

        getColTypeByValue :: Value -> Either ErrorMessage ColumnType
        getColTypeByValue (IntegerValue _) = Right IntegerType
        getColTypeByValue (StringValue _) = Right StringType
        getColTypeByValue (BoolValue _) = Right BoolType
        getColTypeByValue _ = Left "Unknown column type."

        getColType :: Column -> ColumnType
        getColType (Column _ t) = t

-- 4) implement the function which renders a given data frame
-- as ascii-art table (use your imagination, there is no "correct"
-- answer for this task!), it should respect terminal
-- width (in chars, provided as the first argument)
renderDataFrameAsTable :: Integer -> DataFrame -> String
renderDataFrameAsTable _ _ = error "renderDataFrameAsTable not implemented"
