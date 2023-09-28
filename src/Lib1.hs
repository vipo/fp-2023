{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-incomplete-uni-patterns #-}

module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    validateDataFrame,
    renderDataFrameAsTable,
  )
where

import Data.Char ( toLower )

import DataFrame (DataFrame (..), Row, Column (..), ColumnType (..), Value (..))
import InMemoryTables (TableName)

type ErrorMessage = String

type Database = [(TableName, DataFrame.DataFrame)]

-- Your code modifications go below this comment

lowerString :: [Char] -> [Char]
lowerString str = [ toLower loweredString | loweredString <- str]

isPrefixOf' :: (Eq a) => [a] -> [a] -> Bool
isPrefixOf' [] _ = True
isPrefixOf' _ [] = False
isPrefixOf' (x:xs) (y:ys) 
  | null xs && x == y = True
  | x == y            = isPrefixOf' xs ys
  | otherwise         = False

-- 1) implement the function which returns a data frame by its name
-- in provided Database list
findTableByName :: Database -> String -> Maybe DataFrame.DataFrame
findTableByName dataBase tableName = lookup (lowerString tableName) dataBase

-- 2) implement the function which parses a "select * from ..."
-- sql statement and extracts a table name from the statement
parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement query
  | null query = Left "The query is empty"
  | last query /= ';' = Left "The query does not end with a ;"
  | "select * from" `isPrefixOf'` lowerString query = Right (drop 14 (init query))
  | otherwise = Left "The query is wrong"


-- 3) implement the function which validates tables: checks if
-- columns match value types, if rows sizes match columns,..
checkTupleMatch :: [(Column, Value)] -> Bool
checkTupleMatch ((column, value) : _) =
   case (column, value) of
    (Column _ IntegerType, IntegerValue _) -> True
    (Column _ StringType, StringValue _) -> True
    (Column _ BoolType, BoolValue _) -> True
    otherwise -> False

zipColumnsAndValues :: DataFrame -> [(Column, Value)]
zipColumnsAndValues (DataFrame columns rows) = [(col, val) | row <- rows, (col, val) <- zip columns row]

checkRowSizes :: DataFrame -> Bool
checkRowSizes (DataFrame columns rows) = all (\row -> length row == length columns) rows

validateDataFrame :: DataFrame -> Either ErrorMessage ()
validateDataFrame dataFrame
  | not (checkRowSizes dataFrame) = Left "Row sizes do not match"
  | not (checkTupleMatch (zipColumnsAndValues dataFrame)) = Left "Data frame contains type mismatch"
  | otherwise = Right ()

-- 4) implement the function which renders a given data frame
-- as ascii-art table (use your imagination, there is no "correct"
-- answer for this task!), it should respect terminal
-- width (in chars, provided as the first argument)
renderDataFrameAsTable :: Integer -> DataFrame -> String
renderDataFrameAsTable terminalWidth (DataFrame columns rows) =
  let
    -- Calculate the maximum width for each column (headers + values)
    maxColumnWidths = map (maximum . map (valueWidth terminalWidth)) (transposeRows (map (\(Column name _) -> StringValue name) columns : rows))
    
    formatRow :: Row -> String
    formatRow row =
      let
        formattedValues = zipWith3 formatValue maxColumnWidths row columns
      in
        "| " ++ joinWithSeparator " | " formattedValues ++ " |\n"
    formatValue :: Int -> Value -> Column -> String
    formatValue width value (Column _ columnType) =
      let
        paddedValue = case value of
          IntegerValue int -> padRight width (show int)
          StringValue str  -> padRight width str
          BoolValue bool   -> padRight width (show bool)
          NullValue        -> padRight width "NULL"
      in
        paddedValue
    valueWidth :: Integer -> Value -> Int
    valueWidth width value =
      let
        maxValWidth = case value of
          IntegerValue int -> length (show int)
          StringValue str  -> length str
          BoolValue bool   -> length (show bool)
          NullValue        -> length "NULL"
      in
        min (fromIntegral width) maxValWidth
    padRight :: Int -> String -> String
    padRight width str = str ++ replicate (width - length str) ' '
    maxHeaderWidths = map (\(Column name _) -> length name) columns
    header = formatRow (map (\(Column name _) -> StringValue name) columns)
    separator = replicate (sum maxHeaderWidths + 4 * (length maxColumnWidths - 1)) '-' ++ "\n"
  in
    let
      body = concatMap formatRow rows
    in
      separator ++ header ++ separator ++ body ++ separator

transposeRows :: [[a]] -> [[a]]
transposeRows [] = []
transposeRows ([]:_) = []
transposeRows xss = [head' | (head':_) <- xss] : transposeRows [tail' | (_:tail') <- xss]

joinWithSeparator :: String -> [String] -> String
joinWithSeparator _ [] = ""
joinWithSeparator sep (x:xs) = x ++ concatMap (sep ++) xs
