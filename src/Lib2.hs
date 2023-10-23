{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE DataKinds #-}
{-# OPTIONS_GHC -Wno-overlapping-patterns #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Eta reduce" #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# HLINT ignore "Redundant return" #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement (..),
    SelectQuery (..),
    RelationalOperator (..),
    SelectData (..),
    Aggregate (..),
    AggregateFunction (..),
    Expression (..),
    WhereClause (..),
    WhereCriterion (..),
    LogicalOperator (..),
    Value(..)
  )
where

import DataFrame (DataFrame(..), Column(..), ColumnType(..), Value(..), Row)
import InMemoryTables (TableName, database)
import Control.Applicative ( many, some, Alternative(empty, (<|>)), optional )
import Data.Char (toLower, isSpace, isAlphaNum)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]
type ColumnName = String

data RelationalOperator
    = RelEQ
    | RelNE
    | RelLT
    | RelGT
    | RelLE
    | RelGE
    deriving (Show, Eq)


data LogicalOperator
    = And
    deriving (Show, Eq)

data Expression
    = ValueExpression Value
    | ColumnExpression ColumnName
    deriving (Show, Eq)

data WhereCriterion = WhereCriterion Expression RelationalOperator Expression
    deriving (Show, Eq)

data AggregateFunction
    = Min
    | Sum
    deriving (Show, Eq)

data Aggregate = Aggregate AggregateFunction ColumnName
    deriving (Show, Eq)

data SelectData
    = SelectColumn ColumnName
    | SelectAggregate Aggregate
    deriving (Show, Eq)

type SelectQuery = [SelectData]
type WhereClause = [(WhereCriterion, Maybe LogicalOperator)]

-- Keep the type, modify constructors

data ParsedStatement = SelectStatement {
    table :: TableName,
    query :: SelectQuery,
    whereClause :: Maybe WhereClause
} | ShowTableStatement {
    table :: TableName
} | ShowTablesStatement { }
    deriving (Show, Eq)

newtype Parser a = Parser {
    runParser :: String -> Either ErrorMessage (String, a)
}

instance Functor Parser where
    fmap :: (a -> b) -> Parser a -> Parser b
    fmap f p = Parser $ \inp ->
        case runParser p inp of
            Left err -> Left err
            Right (l, a) -> Right (l, f a)

instance Applicative Parser where
    pure :: a -> Parser a
    pure a = Parser $ \inp -> Right (inp, a)
    (<*>) :: Parser (a -> b) -> Parser a -> Parser b
    pf <*> pa = Parser $ \inp1 ->
        case runParser pf inp1 of
            Left err1 -> Left err1
            Right (inp2, f) -> case runParser pa inp2 of
                Left err2 -> Left err2
                Right (inp3, a) -> Right (inp3, f a)

instance Alternative Parser where
    empty :: Parser a
    empty = Parser $ \_ -> Left "Error"
    (<|>) :: Parser a -> Parser a -> Parser a
    p1 <|> p2 = Parser $ \inp ->
        case runParser p1 inp of
            Right a1 -> Right a1
            Left _ -> case runParser p2 inp of
                Right a2 -> Right a2
                Left err -> Left err

instance Monad Parser where
    (>>=) :: Parser a -> (a -> Parser b) -> Parser b
    pa >>= pbGen = Parser $ \inp1 ->
        case runParser pa inp1 of
            Left err1 -> Left err1
            Right (inp2, a) -> case runParser (pbGen a) inp2 of
                Left err2 -> Left err2
                Right (inp3, b) -> Right (inp3, b)

instance Ord Value where
    compare NullValue NullValue = EQ
    compare NullValue _ = LT
    compare _ NullValue = GT
    compare (IntegerValue a) (IntegerValue b) = compare a b
    compare (StringValue a) (StringValue b) = compare a b
    compare (BoolValue a) (BoolValue b) = compare a b

-- Parses user input into an entity representing a parsed
-- statement

parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement inp = case runParser parser (dropWhile isSpace inp) of
    Left err1 -> Left err1
    Right (rest, statement) -> case statement of
        SelectStatement _ _ _ -> case runParser parseEndOfStatement rest of
            Left err2 -> Left err2
            Right _ -> Right statement
        ShowTableStatement _ -> case runParser parseEndOfStatement rest of
            Left err2 -> Left err2
            Right _ -> Right statement
        ShowTablesStatement -> case runParser parseEndOfStatement rest of
            Left err2 -> Left err2
            Right _ -> Right statement
    where
        parser :: Parser ParsedStatement
        parser = parseShowTableStatement
                <|> parseShowTablesStatement
                <|> parseSelectStatement

-- statement by type parsing

parseShowTableStatement :: Parser ParsedStatement
parseShowTableStatement = do
    _ <- parseKeyword "show"
    _ <- parseWhitespace
    _ <- parseKeyword "table"
    _ <- parseWhitespace
    ShowTableStatement <$> parseWord

parseShowTablesStatement :: Parser ParsedStatement
parseShowTablesStatement = do
    _ <- parseKeyword "show"
    _ <- parseWhitespace
    _ <- parseKeyword "tables"
    pure ShowTablesStatement

parseSelectStatement :: Parser ParsedStatement
parseSelectStatement = do
    _ <- parseKeyword "SELECT"
    _ <- parseWhitespace
    selectData <- parseSelectData `sepBy` (parseChar ',' *> optional parseWhitespace)
    _ <- parseWhitespace
    _ <- parseKeyword "FROM"
    _ <- parseWhitespace
    tableName <- parseWord
    whereClause <- optional parseWhereClause
    case validateSelectData selectData of
        Just err -> Parser $ \_ -> Left err
        Nothing -> pure $ SelectStatement tableName selectData whereClause

-- util parsing functions

parseKeyword :: String -> Parser String
parseKeyword keyword = Parser $ \inp ->
    case take (length keyword) inp of
        [] -> Left "Empty input"
        xs
            | map toLower xs == map toLower keyword -> Right (drop (length xs) inp, xs)
            | otherwise -> Left $ "Expected " ++ keyword

parseWhitespace :: Parser String
parseWhitespace = Parser $ \inp ->
    case span isSpace inp of
        ("", _) -> Left $ "Expected whitespace before: " ++ inp
        (whitespace, rest) -> Right (rest, whitespace)

parseEndOfStatement :: Parser String
parseEndOfStatement = do
    _ <- optional parseWhitespace
    _ <- optional (parseChar ';')
    _ <- optional parseWhitespace
    ensureNothingLeft
    where
        ensureNothingLeft :: Parser String
        ensureNothingLeft = Parser $ \inp ->
            case inp of
                [] -> Right ([], [])
                s -> Left ("Characters found after end of SQL statement." ++ s)

parseChar :: Char -> Parser Char
parseChar ch = Parser $ \inp ->
    case inp of
        [] -> Left "Empty input"
        (x:xs) -> if ch == x then Right (xs, ch) else Left ("Expected " ++ [ch])

parseWord :: Parser String
parseWord = Parser $ \inp ->
    case takeWhile (\x -> isAlphaNum x || x == '_') inp of
        [] -> Left "Empty input"
        xs -> Right (drop (length xs) inp, xs)

parseValue :: Parser Value
parseValue = do
    _ <- parseChar '\''
    strValue <- many (parseSatisfy (/= '\''))
    _ <- parseChar '\''
    return $ StringValue strValue
    where
        parseSatisfy :: (Char -> Bool) -> Parser Char
        parseSatisfy predicate = Parser $ \inp ->
            case inp of
                [] -> Left "Empty input"
                (x:xs) -> if predicate x then Right (xs, x) else Left ("Unexpected character: " ++ [x])

--where clause parsing

parseWhereClause :: Parser WhereClause
parseWhereClause = do
    _ <- parseWhitespace
    _ <- parseKeyword "where"
    _ <- parseWhitespace
    some parseCriterionAndOptionalOperator

    where
        parseCriterionAndOptionalOperator :: Parser (WhereCriterion, Maybe LogicalOperator)
        parseCriterionAndOptionalOperator = do
            crit <- parseWhereCriterion
            op <- optional (parseWhitespace >> parseLogicalOperator)
            _ <- optional parseWhitespace
            pure (crit, op)

parseRelationalOperator :: Parser RelationalOperator
parseRelationalOperator =
      (parseKeyword "=" >> pure RelEQ)
  <|> (parseKeyword "!=" >> pure RelNE)
  <|> (parseKeyword "<=" >> pure RelLE)
  <|> (parseKeyword ">=" >> pure RelGE)
  <|> (parseKeyword "<" >> pure RelLT)
  <|> (parseKeyword ">" >> pure RelGT)

parseLogicalOperator :: Parser LogicalOperator
parseLogicalOperator = parseKeyword "AND" >> pure And

parseExpression :: Parser Expression
parseExpression = (ValueExpression <$> parseValue) <|> (ColumnExpression <$> parseWord)

parseWhereCriterion :: Parser WhereCriterion
parseWhereCriterion = do
    leftExpr <- parseExpression <|> Parser (\_ -> Left "Missing left-hand expression in criterion.")
    _ <- optional parseWhitespace
    op <- parseRelationalOperator <|> Parser (\_ -> Left "Missing relational operator.")
    _ <- optional parseWhitespace
    rightExpr <- parseExpression <|> Parser (\_ -> Left "Missing right-hand expression in criterion.")
    pure $ WhereCriterion leftExpr op rightExpr

-- column list parsing

parseColumnNames :: Parser [SelectData]
parseColumnNames = do
    columnNames <- parseWord `sepBy` (parseChar ',' *> optional parseWhitespace)
    return $ map SelectColumn columnNames

sepBy :: Parser a -> Parser b -> Parser [a]
sepBy p sep = do
    x <- p
    xs <- many (sep *> p)
    return (x:xs)

parseSelectData :: Parser SelectData
parseSelectData = tryParseAggregate <|> tryParseColumn
  where
    tryParseAggregate = do
        SelectAggregate <$> parseAggregate
    tryParseColumn = do
        SelectColumn <$> parseWord

-- aggregate parsing

parseAggregateFunction :: Parser AggregateFunction
parseAggregateFunction = parseMin <|> parseSum
  where
    parseMin = do
        _ <- parseKeyword "min"
        pure Min
    parseSum = do
        _ <- parseKeyword "sum"
        pure Sum

parseAggregate :: Parser Aggregate
parseAggregate = do
    func <- parseAggregateFunction
    _ <- optional parseWhitespace
    _ <- parseChar '('
    _ <- optional parseWhitespace
    columnName <- parseWord
    _ <- optional parseWhitespace
    _ <- parseChar ')'
    pure $ Aggregate func columnName

-- validation

validateSelectData :: [SelectData] -> Maybe ErrorMessage
validateSelectData selectData
    | all isSelectColumn selectData = Nothing
    | all isSelectAggregate selectData = Nothing
    | otherwise = Just "Mixing columns and aggregate functions in SELECT is not allowed."

isSelectColumn :: SelectData -> Bool
isSelectColumn (SelectColumn _) = True
isSelectColumn _ = False

isSelectAggregate :: SelectData -> Bool
isSelectAggregate (SelectAggregate _) = True
isSelectAggregate _ = False

--util functions

getTableByName :: TableName -> Either ErrorMessage DataFrame
getTableByName tableName =
  case lookup tableName database of
    Just table -> Right table
    Nothing -> Left $ "Table with name '" ++ tableName ++ "' does not exist in the database."

findColumnIndex :: ColumnName -> [Column] -> Either ErrorMessage Int
findColumnIndex columnName columns = findColumnIndex' columnName columns 0 -- Start with index 0

findColumnIndex' :: ColumnName -> [Column] -> Int -> Either ErrorMessage Int
findColumnIndex' columnName [] _ = Left $ "Column with name '" ++ columnName ++ "' does not exist in the table."
findColumnIndex' columnName ((Column name _):xs) index
    | columnName == name = Right index
    | otherwise          = findColumnIndex' columnName xs (index + 1)

extractColumn :: Int -> [Row] -> [Value]
extractColumn columnIndex rows = [values !! columnIndex | values <- rows, length values > columnIndex]

findColumnType :: ColumnName -> [Column] -> Either ErrorMessage ColumnType
findColumnType columnName columns = findColumnType' columnName columns 0

findColumnType' :: ColumnName -> [Column] -> Int -> Either ErrorMessage ColumnType
findColumnType' _ [] _ = Left "Column does not exist in the table."
findColumnType' columnName (column@(Column name columnType):xs) index
    | columnName == name = Right columnType
    | otherwise          = findColumnType' columnName xs (index+1)

minColumnValue :: Int -> [Row] -> Either ErrorMessage Value
minColumnValue index rows =
    case findMin $ extractColumn index rows of
        Left message -> Left message
        Right value -> Right value

    where

        findMin :: [Value] -> Either ErrorMessage Value
        findMin values =
            case filter (/= NullValue) values of
                [] -> Left "Column has no values."
                vals -> Right (minValue vals)

        minValue :: [Value] -> Value
        minValue = foldl1 minValue'

        minValue' :: Value -> Value -> Value
        minValue' (IntegerValue a) (IntegerValue b) = IntegerValue (min a b)
        minValue' (StringValue a) (StringValue b) = StringValue (min a b)
        minValue' (BoolValue a) (BoolValue b) = BoolValue (a && b)
        minValue' _ _ = NullValue

sumColumnValues :: Int -> [Row] -> ColumnName -> [Column] -> Either ErrorMessage Value
sumColumnValues index rows columnName columns =
    case findSumColumnType columnName columns of
        Left errorMessage -> Left errorMessage
        Right columnType ->
            case columnType of
                IntegerType -> findSum $ extractColumn index rows
                _ -> Left "Column type is not Integer."

    where

        findSumColumnType :: ColumnName -> [Column] -> Either ErrorMessage ColumnType
        findSumColumnType cName allColumns =
            case [columnType | (Column name columnType) <- allColumns, name == cName] of
                [] -> Left "Column does not exist."
                (columnType:_) -> Right columnType

        findSum :: [Value] -> Either ErrorMessage Value
        findSum values =
            case filter (/= NullValue) values of
                [] -> Left "Column has no values."
                vals -> Right (sumValues vals)

        sumValues :: [Value] -> Value
        sumValues = foldl sumValues' (IntegerValue 0)

        sumValues' :: Value -> Value -> Value
        sumValues' (IntegerValue a) (IntegerValue b) = IntegerValue (a + b)
        sumValues' (IntegerValue a) NullValue = IntegerValue (a)
        sumValues' NullValue (IntegerValue b) = IntegerValue (b)
        sumValues' _ _ = NullValue

-- Executes a parsed statement. Produces a DataFrame. Uses
-- InMemoryTables.databases as a source of data.

executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement ShowTablesStatement = Right $ convertToDataFrame (tableNames database)

executeStatement (ShowTableStatement tableName) =
    case getTableByName tableName of
        Left errorMessage -> Left errorMessage
        Right df -> Right df

executeStatement (SelectStatement tableName selectQuery maybeWhereClause) =
    case getTableByName tableName of
        Left errorMessage -> Left errorMessage
        Right (DataFrame columns rows) ->
            case executeSelectQuery selectQuery columns rows maybeWhereClause of
                Left selectError -> Left selectError
                Right (selectedColumns, filteredRows) ->
                    Right $ DataFrame selectedColumns filteredRows

executeSelectQuery :: SelectQuery -> [Column] -> [Row] -> Maybe WhereClause -> Either ErrorMessage ([Column], [Row])
executeSelectQuery selectQuery columns rows maybeWhereClause =
    case maybeWhereClause of
        Nothing -> processSelectQuery selectQuery columns rows
        Just whereClause -> do
            filteredRows <- filterRowsByWhereClause whereClause columns rows
            processSelectQuery selectQuery columns filteredRows

processSelectQuery :: SelectQuery -> [Column] -> [Row] -> Either ErrorMessage ([Column], [Row])
processSelectQuery [] _ _ = Left "No columns or aggregates selected in the SELECT statement."
processSelectQuery selectQuery columns rows = do
    (selectedColumns, selectedRows) <- processSelectQuery' selectQuery columns rows [] []
    if null selectedColumns
        then Left "No valid columns or aggregates selected in the SELECT statement."
        else Right (selectedColumns, selectedRows)

processSelectQuery' :: SelectQuery -> [Column] -> [Row] -> [Column] -> [Int] -> Either ErrorMessage ([Column], [Row])
processSelectQuery' [] _ rows selectedColumns selectedIndices = Right (reverse selectedColumns, filterRows (reverse selectedIndices) rows)
processSelectQuery' (selectData:rest) columns rows selectedColumns selectedIndices =
    case selectData of
        SelectColumn columnName -> do
            columnIndex <- findColumnIndex columnName columns
            processSelectQuery' rest columns rows (columns !! columnIndex : selectedColumns) (columnIndex : selectedIndices)
        SelectAggregate (Aggregate aggFunc columnName) -> do
            columnIndex <- findColumnIndex columnName columns
            columnType <- findColumnType columnName columns
            let newColumn = createAggregateColumn aggFunc columnName columnType
            let newRows = createAggregateRows columnName columns aggFunc columnIndex rows
            processSelectQuery' rest columns newRows (newColumn : selectedColumns) (columnIndex : selectedIndices)

createAggregateColumn :: AggregateFunction -> ColumnName -> ColumnType -> Column
createAggregateColumn aggFunc columnName columnType =
    case aggFunc of
        Min -> Column ("MIN(" ++ columnName ++ ")") columnType
        Sum -> Column ("SUM(" ++ columnName ++ ")") IntegerType

createAggregateRows :: ColumnName -> [Column] -> AggregateFunction -> Int -> [Row] -> [Row]
createAggregateRows columnName columns aggFunc index rows =
    case aggFunc of
        Sum -> let sumValue = sumColumnValues index rows columnName columns in
            case sumValue of
            Right value -> take 1 $ map (updateCell index value) rows
        Min -> let minValue = minColumnValue index rows in
            case minValue of
            Right value -> take 1 $ map (updateCell index value) rows

updateCell :: Int -> a -> [a] -> [a]
updateCell index newValue row =
    take index row ++ [newValue] ++ drop (index + 1) row

filterRows :: [Int] -> [Row] -> [Row]
filterRows indices rows = [selectColumns indices row | row <- rows]
  where
    selectColumns :: [Int] -> Row -> Row
    selectColumns [] _ = []
    selectColumns (i:rest) row = (row !! i) : selectColumns rest row

filterRowsByWhereClause :: WhereClause -> [Column] -> [Row] -> Either ErrorMessage [Row]
filterRowsByWhereClause [] _ rows = Right rows
filterRowsByWhereClause ((criterion, _):rest) columns rows = do
    filteredRows <- filterRowsWithCriterion criterion columns rows []
    case filteredRows of
        [] -> Left "No rows satisfy criterion."
        _ -> filterRowsByWhereClause rest columns filteredRows

filterRowsWithCriterion :: WhereCriterion -> [Column] -> [Row] -> [Row] -> Either ErrorMessage [Row]
filterRowsWithCriterion _ _ [] acc = Right acc
filterRowsWithCriterion criterion columns (row:rest) acc =
    if evaluateCriterion criterion columns row
        then filterRowsWithCriterion criterion columns rest (row : acc)
        else filterRowsWithCriterion criterion columns rest acc

evaluateCriterion :: WhereCriterion -> [Column] -> Row -> Bool
evaluateCriterion (WhereCriterion leftExpr relOp rightExpr) columns row =
    let
        leftValue = evaluateExpression leftExpr columns row
        rightValue = evaluateExpression rightExpr columns row
    in
        case relOp of
            RelEQ -> leftValue == rightValue
            RelNE -> leftValue /= rightValue
            RelLT -> leftValue < rightValue
            RelGT -> leftValue > rightValue
            RelLE -> leftValue <= rightValue
            RelGE -> leftValue >= rightValue

evaluateExpression :: Expression -> [Column] -> Row -> Value
evaluateExpression (ValueExpression value) _ _ = value
evaluateExpression (ColumnExpression columnName) columns row =
    case lookupColumnValue columnName columns row of
        Just value -> value
        Nothing -> NullValue

lookupColumnValue :: ColumnName -> [Column] -> Row -> Maybe Value
lookupColumnValue columnName columns row =
    case findColumnIndex columnName columns of
        Left _ -> Nothing
        Right columnIndex -> Just (row !! columnIndex)

tableNames :: Database -> [TableName]
tableNames db = map fst db

convertToDataFrame :: [TableName] -> DataFrame
convertToDataFrame alltableNames = DataFrame [Column "Table Name" StringType] (map (\name -> [StringValue name]) alltableNames)
