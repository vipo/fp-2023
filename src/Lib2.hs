{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE DataKinds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement (..)
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

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement inp = case runParser parser (dropWhile isSpace inp) of
    Left err1 -> Left err1
    Right (rest, statement) -> case runParser parseEndOfStatement rest of
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
    selectData <- parseSelectData `sepBy` (parseChar ',' *> parseWhitespace)
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
  <|> (parseKeyword "<" >> pure RelLT)
  <|> (parseKeyword ">" >> pure RelGT)
  <|> (parseKeyword "<=" >> pure RelLE)
  <|> (parseKeyword ">=" >> pure RelGE)

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
    _ <- parseChar '('
    columnName <- parseWord
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


--util functions for SELECT statement

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


minColumnValue :: TableName -> ColumnName -> Either ErrorMessage Value
minColumnValue tableName columnName =
    case getTableByName tableName of
        Left errorMessage -> Left errorMessage
        Right (DataFrame columns rows) ->
            case findColumnIndex columnName columns of
                Left errorMessage -> Left errorMessage
                Right columnIndex -> findMin $ extractColumn columnIndex rows

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


sumColumnValues :: TableName -> ColumnName -> Either ErrorMessage Value
sumColumnValues tableName columnName =
    case getTableByName tableName of
        Left errorMessage -> Left errorMessage
        Right (DataFrame columns rows) ->
            case findColumnType columnName columns of
                Left errorMessage -> Left errorMessage
                Right columnType ->
                    case columnType of
                        IntegerType ->
                            case findColumnIndex columnName columns of
                                Left errorMessage -> Left errorMessage
                                Right columnIndex -> findSum $ extractColumn columnIndex rows
                        _ -> Left "Column type is not Integer."

    where

        findColumnType :: ColumnName -> [Column] -> Either ErrorMessage ColumnType
        findColumnType columnName columns =
            case [columnType | (Column name columnType) <- columns, name == columnName] of
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
        sumValues' (IntegerValue a) NullValue = IntegerValue a
        sumValues' NullValue (IntegerValue b) = IntegerValue b
        sumValues' _ _ = NullValue

-- Executes a parsed statement. Produces a DataFrame. Uses
-- InMemoryTables.databases as a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement ShowTablesStatement = Right $ convertToDataFrame (tableNames database)
executeStatement _ = Left "Not implemented: executeStatement for other statements"

tableNames :: Database -> [TableName]
tableNames = map fst

convertToDataFrame :: [TableName] -> DataFrame
convertToDataFrame tableNames = DataFrame [Column "Table Name" StringType] (map (\name -> [StringValue name]) tableNames)