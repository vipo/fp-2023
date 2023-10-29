{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement
  )
where

import DataFrame (Column (..), ColumnType (..), Value (..), DataFrame (..), Row (..))
import InMemoryTables (TableName, database)
import Data.List (find, elemIndex, isPrefixOf)
import Lib1 ()
import Data.Char (toLower, isSpace)
import Data.Maybe (fromJust)
import Text.Read (readMaybe)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

data Operator = Operator String String Value

-- Keep the type, modify constructors
data ParsedStatement
  = ShowTables
  | ShowTable TableName
  | Select [String] TableName (Maybe Operator)
  | ParsedStatement
  | Min String TableName String (Maybe Operator)
  | Sum String TableName String (Maybe Operator)

instance Ord Value where
  compare (StringValue s1) (StringValue s2) = compare s1 s2
  compare _ _ = EQ  -- Handle other Value constructors, e.g., handle Null or other types

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input
  | null input = Left "Empty input"
  | last input == ';' = parseStatement (init input)
  | toLowerPrefix "select min(" input =
    case words (map toLower input) of
      ["select", columnName, "from", table] -> Right (Min columnName table "minimum" Nothing)
      ("select" : columnName : "from" : table : "where" : rest) -> do
        (conditions, _) <- parseWhereConditions rest
        Right (Min columnName table "minimum" (Just conditions))
      _ -> Left "Invalid MIN statement"
  | toLowerPrefix "select sum(" input =
    case words (map toLower input) of
      ["select", columnName, "from", table] -> Right (Sum columnName table "sum" Nothing)
      ("select" : columnName : "from" : table : "where" : rest) -> do
        (conditions, _) <- parseWhereConditions rest
        Right (Sum columnName table "sum" (Just conditions))
      _ -> Left "Invalid SUM statement"
  | otherwise =
    case words (map toLower input) of
      ["show", "tables"] -> Right ShowTables
      ["show", "table", table] -> Right (ShowTable table)
      ("select" : columns) ->
        case break (== "from") columns of
          (cols, "from" : tableName : "where" : rest) -> do
            (conditions, _) <- parseWhereConditions rest
            Right (Select cols tableName (Just conditions))
          (cols, "from" : tableName : _) -> Right (Select cols tableName Nothing)
          _ -> Left "Invalid SELECT statement"
      _ -> Left "Not supported statement"

toLowerPrefix :: String -> String -> Bool
toLowerPrefix prefix str = isPrefixOf (map toLower prefix) (map toLower str)

parseWhereConditions :: [String] -> Either ErrorMessage (Operator, [String])
-- parseWhereConditions [] = Right (And [], [])
parseWhereConditions [] = Left "Invalid WHERE statement"
parseWhereConditions (colName : op : value : rest) =
  case op of
    "=" -> case readMaybe value of
      Just intValue -> Right (Operator colName "=" (IntegerValue intValue), rest)
      Nothing -> case value of
        "true" -> Right (Operator colName "=" (BoolValue True), rest)
        "false" -> Right (Operator colName "=" (BoolValue False), rest)
        "null" -> Right (Operator colName "=" NullValue, rest)
        _ -> Right (Operator colName "=" (StringValue value), rest)
    "<" -> case readMaybe value of
      Just intValue -> Right (Operator colName "<" (IntegerValue intValue), rest)
      Nothing -> Right (Operator colName "<" (StringValue value), rest)
    ">" -> case readMaybe value of
      Just intValue -> Right (Operator colName ">" (IntegerValue intValue), rest)
      Nothing -> Right (Operator colName ">" (StringValue value), rest)
    "<=" -> case readMaybe value of
      Just intValue -> Right (Operator colName "<=" (IntegerValue intValue), rest)
      Nothing -> Right (Operator colName "<=" (StringValue value), rest)
    ">=" -> case readMaybe value of
      Just intValue -> Right (Operator colName ">=" (IntegerValue intValue), rest)
      Nothing -> Right (Operator colName ">=" (StringValue value), rest)
    _ -> Left "Invalid operator"
parseWhereConditions (_ : _) = Left "Invalid WHERE statement"

-- parseWhereConditions (colName : rest) = Right (Equal colName (StringValue (head rest)), tail rest)


-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
-- ExecuteStatement function
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement ShowTables = Right showTables
executeStatement (ShowTable tablename) =
  case lookup (map toLower tablename) database of
    Just df -> Right $ DataFrame [Column "columns" StringType] (map (\col -> [StringValue (columnName col)]) (columns df))
    Nothing -> Left "Table not found"
executeStatement (Select columnNames tableName maybeOperator) =
  case maybeOperator of
    Just (Operator colName op compCase) -> case lookup (map toLower tableName) database of -- WHERE clause provided
      Just df -> do
        let filteredDataFrame = filterDataFrameByColumnValue colName op compCase df
        let filteredRows = rows filteredDataFrame
        let selectedCols = if "*" `elem` columnNames
                          then columns df
                          else filter (\col -> columnName col `elem` columnNames) (columns df)
        let selectedIndices = map (columnIndex df) selectedCols
        let selectedRows = map (\row -> map (\i -> row !! i) selectedIndices) filteredRows
        Right $ DataFrame selectedCols selectedRows
      Nothing -> Left "Table not found"
    Nothing -> case lookup (map toLower tableName) database of -- No WHERE clause provided
      Just df -> do
        let filteredRows = rows df
        let selectedCols = if "*" `elem` columnNames
                          then columns df
                          else filter (\col -> columnName col `elem` columnNames) (columns df)
        let selectedIndices = map (columnIndex df) selectedCols
        let selectedRows = map (\row -> map (\i -> row !! i) selectedIndices) filteredRows
        Right $ DataFrame selectedCols selectedRows
      Nothing -> Left "Table not found"
executeStatement (Min columnName tableName resultColumn maybeOperator) =
  case maybeOperator of -- WHERE clause provided
    Just (Operator colName op compCase) -> case lookup (map toLower tableName) database of
      Just df -> do
        let colIndex = columnIndex df (Column columnName StringType)
        let filteredDataFrame = filterDataFrameByColumnValue colName op compCase df
        let filteredRows = rows filteredDataFrame
        let minValResult = case filteredRows of
                          [] -> NullValue
                          _ -> foldl1 (minValue colIndex (columnType (columns df !! colIndex))) (map (!! colIndex) filteredRows)
        Right $ DataFrame [Column resultColumn (columnType (columns df !! colIndex))] [[minValResult]]
      Nothing -> Left "Table not found"
    Nothing -> case lookup (map toLower tableName) database of
      Just df -> do
        let colIndex = columnIndex df (Column columnName StringType)
        let nonNullRows = filter (\row -> case row !! colIndex of { NullValue -> False; _ -> True }) (rows df)
        let minValResult = case nonNullRows of
                          [] -> NullValue
                          _ -> foldl1 (minValue colIndex (columnType (columns df !! colIndex))) (map (!! colIndex) nonNullRows)
        Right $ DataFrame [Column resultColumn (columnType (columns df !! colIndex))] [[minValResult]]
      Nothing -> Left "Table not found"

executeStatement (Sum columnName tableName resultColumn maybeOperator) =
  case maybeOperator of -- WHERE clause provided
    Just (Operator colName op compCase) -> case lookup (map toLower tableName) database of
      Just df -> do
        let colIndex = columnIndex df (Column columnName StringType)
        let filteredDataFrame = filterDataFrameByColumnValue colName op compCase df
        let filteredRows = rows filteredDataFrame
        let sumVal = calculateSum colIndex filteredRows
        Right $ DataFrame [Column resultColumn (columnType (columns df !! colIndex))] [[sumVal]]
      Nothing -> Left "Table not found"
    Nothing -> case lookup (map toLower tableName) database of
      Just df -> do
        let colIndex = columnIndex df (Column columnName StringType)
        let sumVal = calculateSum colIndex (rows df)
        Right $ DataFrame [Column resultColumn (columnType (columns df !! colIndex))] [[sumVal]]
      Nothing -> Left "Table not found"



executeStatement _ = Left "Not implemented"

-- Helper functions
showTables :: DataFrame
showTables = DataFrame [Column "tables" StringType] (map (return . StringValue . fst) database)



maybeTableToEither :: Maybe DataFrame -> Either ErrorMessage DataFrame
maybeTableToEither (Just df) = Right df
maybeTableToEither Nothing = Left "Table not found"

maybeFilterRows :: DataFrame -> Maybe Operator -> Maybe [Row]
maybeFilterRows df (Just (Operator colName op compCase)) = case Just (Operator colName op compCase) of
  Just condition -> Just $ filter (evalCondition op colName compCase df) (rows df)
  Nothing -> Just $ rows df
maybeFilterRows _ Nothing = Nothing




filterColumns :: [String] -> DataFrame -> [Column]
filterColumns columnNames df = filter (\col -> columnName col `elem` columnNames) (columns df)

selectRow :: DataFrame -> [Column] -> Row -> Row
selectRow df selectedCols row = map (\col -> row !! columnIndex df col) selectedCols

evalCondition :: String -> String -> Value -> DataFrame -> Row -> Bool
-- evalCondition "and" colName val df row = any (\cond -> evalCondition cond df row) conditions
evalCondition colName "=" val df row = matchValue (getColumnValue colName df row) val
evalCondition colName "<" val df row = compareValues (<) colName val df row
evalCondition colName ">" val df row = compareValues (>) colName val df row
evalCondition colName "<=" val df row = compareValues (<=) colName val df row
evalCondition colName ">=" val df row = compareValues (>=) colName val df row
evalCondition _ _ _ _ _ = False

columnType :: Column -> ColumnType
columnType (Column _ colType) = colType

-- Function to filter a DataFrame based on a specified column value
filterDataFrameByColumnValue :: String -> String -> Value -> DataFrame -> DataFrame
filterDataFrameByColumnValue colName op val df = do
  let filteredRows = filter (\row -> evalCondition colName op val df row) (rows df)
  DataFrame (columns df) filteredRows{- matchValue (getColumnValue colName df row) val) (rows df) -}

getColumn :: String -> DataFrame -> Column
getColumn colName df = case find (\col -> columnName col == colName) (columns df) of
  Just col -> col
  Nothing -> error "Column not found"

-- Helper function to find the column index by name
findColumnIndex :: String -> [Column] -> Maybe Int
findColumnIndex colName cols = elemIndex colName (map columnName cols)

matchValue :: Value -> Value -> Bool
matchValue (StringValue s1) (StringValue s2) = map toLower s1 == map toLower s2
matchValue (IntegerValue i1) (IntegerValue i2) = i1 == i2
matchValue (BoolValue b1) (BoolValue b2) = b1 == b2
matchValue NullValue NullValue = True
matchValue _ _ = False

trimValue :: Value -> Value
trimValue (StringValue s) = StringValue (trimWhitespace s)
trimValue v = v

getColumnValue :: String -> DataFrame -> Row -> Value
getColumnValue colName df row =
  let colIndex = columnIndex df (Column (trimWhitespace (map toLower colName)) StringType)
  in row !! colIndex

trimWhitespace :: String -> String
trimWhitespace = filter (not . isSpace)

compareValues :: (String -> String -> Bool) -> String -> Value -> DataFrame -> Row -> Bool
compareValues op colName val df row =
  case (trimValue (getColumnValue colName df row), trimValue val) of
    (StringValue s1, StringValue s2) -> op (map toLower s1) (map toLower s2)
    (IntegerValue i1, IntegerValue i2) -> op (show i1) (show i2)
    _ -> False

columnName :: Column -> String
columnName (Column name _)
  | toLowerPrefix "min(" name = init $ tail $ dropWhile (/= '(') name
  | toLowerPrefix "sum(" name = init $ tail $ dropWhile (/= '(') name
columnName (Column name _) = name

columnIndex :: DataFrame -> Column -> Int
columnIndex (DataFrame cols _) col = case find (\c -> columnName c == columnName col) cols of
  Just foundCol -> fromJust $ elemIndex foundCol cols
  -- if Nothing then return error "Column not found" and the name of the column that was not found
  Nothing -> error $ "Column not found: " ++ columnName col

rows :: DataFrame -> [Row]
rows (DataFrame _ rs) = rs

columns :: DataFrame -> [Column]
columns (DataFrame cols _) = cols





minVal :: String -> DataFrame -> Maybe Value
minVal colName (DataFrame cols rows) = do
  colIndex <- findColumnIndex colName cols
  let colValues = map (\row -> row !! colIndex) rows
  case colValues of
    [] -> Nothing  -- Empty column
    _  -> Just (minimum colValues)

minValue :: Int -> ColumnType -> Value -> Value -> Value
minValue _ IntegerType (IntegerValue i1) (IntegerValue i2) = if i1 < i2 then IntegerValue i1 else IntegerValue i2
minValue _ BoolType (BoolValue b1) (BoolValue b2) = if b1 < b2 then BoolValue b1 else BoolValue b2
minValue _ StringType (StringValue s1) (StringValue s2) = if s1 < s2 then StringValue s1 else StringValue s2
minValue _ _ val1 NullValue = val1
minValue _ _ NullValue val2 = val2
minValue colIndex colType _ _ = error $ "Column type mismatch for column at index " ++ show colIndex ++ ". Expected " ++ show colType

calculateSum :: Int -> [Row] -> Value
calculateSum colIndex rows =
  let totalSum = sum [case row !! colIndex of { IntegerValue i -> i; _ -> 0 } | row <- rows]
      count = toInteger (length rows)
  in if count > 0 then IntegerValue (totalSum) else NullValue