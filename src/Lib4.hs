{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE InstanceSigs #-}

module Lib4() where
import Lib2(stopParseAt, dropWhiteSpaces, getOperand, stringToInt, isNumber, areSpacesBetweenWords, splitStatementAtParentheses, tableNameParser)
import Control.Applicative(Alternative(empty, (<|>)),optional, some, many)
import Control.Monad.Trans.State.Strict (State, StateT, get, put, runState, runStateT, state)
import Data.Char (toLower, GeneralCategory (ParagraphSeparator), isSpace, isAlphaNum, isDigit, digitToInt)
import Data.String (IsString, fromString)
import Control.Monad.Trans.Class
import Control.Monad.Trans.State.Strict
import DataFrame as DF
import Data.Char (isDigit)
import GHC.IO.Handle (NewlineMode(inputNL))
import Data.List (isPrefixOf, nub)

type ColumnName = String
type Error = String
type Parser a = EitherT Error (State String) a
data OrderBy = OrderByColumnName [ColumnName] | OrderByColumnNumber [Integer]
  deriving (Show, Eq)
type TableName = String
type FileContent = String
type DeserializedContent = (TableName, DataFrame)
type ErrorMessage = String
type TableArray =  [TableName]

data Trash = Trash String
  deriving (Show, Eq)

data NowFunction = Now
  deriving (Show, Eq)

data Aggregate2 = AggregateColumn (AggregateFunction, ColumnName) | AggregateColumnTable (AggregateFunction, (TableName, ColumnName))
  deriving (Show, Eq)

data SpecialSelect2 = SelectAggregate2 [Aggregate2] (Maybe NowFunction) | SelectColumn2 [ColumnName] (Maybe NowFunction) | SelectedColumnsTables [(TableName, ColumnName)] (Maybe NowFunction)
  deriving (Show, Eq)

data AggregateFunction = Sum | Max
  deriving (Show, Eq)

data And = And
  deriving (Show, Eq)

type AggregateList = [(AggregateFunction, ColumnName)]

data SpecialSelect = SelectAggregate AggregateList | SelectColumns [ColumnName]
  deriving (Show, Eq)

data Operand = ColumnOperand ColumnName | ConstantOperand Value | ColumnTableOperand (TableName, ColumnName)
  deriving (Show, Eq)

data Operator =
     IsEqualTo
    |IsNotEqual
    |IsLessThan
    |IsGreaterThan
    |IsLessOrEqual
    |IsGreaterOrEqual
    deriving (Show, Eq)

data Condition = Condition Operand Operator Operand
  deriving (Show, Eq)

type WhereSelect = [Condition]

data SelectedColumns = ColumnsSelected [ColumnName]
  deriving (Show, Eq)

type InsertedValues = [DF.Value]

data ParsedStatement3 =
  SelectNow {}
  | DropTable {
    table :: TableName
  }
  | CreateTable {
    table :: TableName,
    newColumns :: [Column]
  }
  | Insert {
    table :: TableName,
    columns :: Maybe SelectedColumns,
    values :: InsertedValues
  }
  | Update {
    table :: TableName,
    selectUpdate :: WhereSelect,
    selectWhere :: Maybe WhereSelect
  }
  | Delete {
    table :: TableName,
    conditions :: Maybe WhereSelect
  }
  | ShowTable {
    table :: TableName
   }
  | SelectAll {
    tables :: TableArray,
    selectWhere :: Maybe WhereSelect,
    order :: Maybe OrderBy
   }
  |Select {
    selectQuery :: SpecialSelect2,
    tables :: TableArray,
    selectWhere :: Maybe WhereSelect,
    order :: Maybe OrderBy
  }
  | ShowTables { }
    deriving (Show, Eq)


newtype EitherT e m a = EitherT {
    runEitherT :: m (Either e a)
}

instance MonadTrans (EitherT e) where
  lift :: Monad m => m a -> EitherT e m a
  lift ma = EitherT $ fmap Right ma

instance Monad m => Functor (EitherT e m) where
  fmap :: (a -> b) -> EitherT e m a -> EitherT e m b
  fmap f ta = EitherT $ do
    eit <- runEitherT ta
    case eit of
      Left e -> return $ Left e
      Right a -> return $ Right (f a)

instance Monad m => Applicative (EitherT e m) where
  pure :: a -> EitherT e m a
  pure a = EitherT $ return $ Right a
  (<*>) :: EitherT e m (a -> b) -> EitherT e m a -> EitherT e m b
  af <*> aa = EitherT $ do
    f <- runEitherT af
    case f of
      Left e1 -> return $ Left e1
      Right r1 -> do
        a <- runEitherT aa
        case a of
          Left e2 -> return $ Left e2
          Right r2 -> return $ Right (r1 r2)

instance Monad m => Monad (EitherT e m) where
  (>>=) :: EitherT e m a -> (a -> EitherT e m b) -> EitherT e m b
  m >>= k = EitherT $ do
    eit <- runEitherT m
    case eit of
      Left e -> return $ Left e
      Right r -> runEitherT (k r)

throwE :: Monad m => e -> EitherT e m a
throwE err = EitherT $ return $ Left err


-- parseStatement3 :: String -> Either ErrorMessage ParsedStatement3
-- parseStatement2 query = case runParser p query of
--     Left err1 -> Left err1
--     Right (query, rest) -> case query of
--         Lib3.Select _ _ _ -> case runParser stopParseAt rest of
--           Left err2 -> Left err2
--           Right _ -> Right query
--         Lib3.ShowTable _ -> case runParser stopParseAt rest of
--           Left err2 -> Left err2
--           Right _ -> Right query
--         Lib3.ShowTables -> case runParser stopParseAt rest of
--           Left err2 -> Left err2
--           Right _ -> Right query
--         Lib3.SelectAll _ _ -> case runParser stopParseAt rest of
--           Left err2 -> Left err2
--           Right _ -> Right query
--         Lib3.Insert _ _ _ -> case runParser stopParseAt rest of
--           Left err2 -> Left err2
--           Right _ -> Right query
--         Lib3.Update _ _ _-> case runParser stopParseAt rest of
--           Left err2 -> Left err2
--           Right _ -> Right query
--         Lib3.Delete _ _ -> case runParser stopParseAt rest of
--           Left err2 -> Left err2
--           Right _ -> Right query
--         Lib3.SelectNow -> case runParser stopParseAt rest of
--           Left err2 -> Left err2
--           Right _ -> Right query
--         CreateTable _ _ -> case stopParseAt rest of
--           Left err2 -> Left err2
--           Right _ -> Right query
--         DropTable _  -> case stopParseAt rest of
--           Left err2 -> Left err2
--           Right _ -> Right query
--     where
--         p :: Parser ParsedStatement3
--         p = showTableParser
--                <|> showTablesParser
--                <|> selectStatementParser
--                <|> selectAllParser
--                <|> insertParser
--                <|> updateParser
--                <|> deleteParser
--                <|> selectNowParser
--                <|> createTableParser
--                <|> dropTableParser

dropTableParser :: Parser ParsedStatement3
dropTableParser = do
    _ <- queryStatementParser "drop"
    _ <- whitespaceParser
    _ <- queryStatementParser "table"
    _ <- whitespaceParser
    table <- columnNameParser 
    pure $ DropTable table

createTableParser :: Parser ParsedStatement3
createTableParser = do
    _ <- queryStatementParser "create"
    _ <- whitespaceParser
    _ <- queryStatementParser "table"
    _ <- whitespaceParser
    table <- columnNameParser
    _ <- whitespaceParser
    _ <- char '('
    _ <- optional whitespaceParser
    columnsAndTypes <- columnListParser
    _ <- optional whitespaceParser
    _ <- char ')'
    _ <- optional whitespaceParser
    pure $ CreateTable table columnsAndTypes

runParser :: Parser a -> String -> Either Error (a, String)
runParser parser input = 
    let eitherResult = runState (runEitherT parser) input
    in case eitherResult of
        (Left errMsg, _) -> Left errMsg
        (Right value, remainingInput) -> Right (value, remainingInput)

instance (IsString e) => Alternative (EitherT e (State s)) where
    empty :: EitherT e (State s) a
    empty = EitherT $ state $ \s -> (Left (fromString "Error"), s)

    (<|>) :: EitherT e (State s) a -> EitherT e (State s) a -> EitherT e (State s) a
    a1 <|> a2 = EitherT $ state $ \s ->
        let result = runState (runEitherT a1) s
        in case result of
            (Left _, _) -> runState (runEitherT a2) s
            _ -> result

----------------------------------------------------------------------------------

insertParser :: Parser ParsedStatement3
insertParser = do
    _ <- queryStatementParser "insert"
    _ <- whitespaceParser
    _ <- queryStatementParser "into"
    _ <- whitespaceParser
    tableName <- columnNameParser
    _ <- optional whitespaceParser
    selectUpdate <- optional selectDataParsers
    _ <- optional whitespaceParser
    _ <- queryStatementParser "values"
    _ <- optional whitespaceParser
    _ <- queryStatementParser "("
    values <- insertedValuesParser
    _ <- queryStatementParser ")"
    pure $ Insert tableName selectUpdate values

selectDataParsers :: Parser SelectedColumns
selectDataParsers = tryParseColumn
  where
    tryParseColumn = do
      _ <- queryStatementParser "("
      columnNames <- seperate columnNameParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      _ <- queryStatementParser ")"
      return $ ColumnsSelected columnNames

insertedValuesParser :: Parser InsertedValues
insertedValuesParser = do
  values <- seperate valueParser (queryStatementParser "," >> optional whitespaceParser)
  return values

valueParser :: Parser DF.Value
valueParser = parseNullValue <|> parseBoolValue <|> parseStringValue <|> parseNumericValue

parseNullValue :: Parser DF.Value
parseNullValue = queryStatementParser "null" >> return NullValue

parseBoolValue :: Parser DF.Value
parseBoolValue =
  (queryStatementParser "true" >> return (BoolValue True))
  <|> (queryStatementParser "false" >> return (BoolValue False))

parseStringValue :: Parser DF.Value
parseStringValue = do
  strValue <- parseStringWithQuotes
  return (StringValue strValue)

parseStringWithQuotes :: Parser String
parseStringWithQuotes = do
  _ <- queryStatementParser "'"
  str <- some (parseSatisfy (/= '\''))
  _ <- queryStatementParser "'"
  return str

parseNumericValue :: Parser DF.Value
parseNumericValue = IntegerValue <$> parseInt

parseInt :: Parser Integer
parseInt = do
  digits <- some (parseSatisfy isDigit)
  return (read digits)

deleteParser :: Parser ParsedStatement3
deleteParser = do
    _ <- queryStatementParser "delete"
    _ <- whitespaceParser
    _ <- queryStatementParser "from"
    _ <- whitespaceParser
    tableName <- columnNameParser
    conditions <- optional whereParser
    pure $ Delete tableName conditions

updateParser :: Parser ParsedStatement3
updateParser = do
    _ <- queryStatementParser "update"
    _ <- whitespaceParser
    tableName <- columnNameParser
    selectUpdated <- setParser
    selectedWhere <- optional whereParser
    pure $ Update tableName selectUpdated selectedWhere

setParser :: Parser WhereSelect
setParser = do
  _ <- whitespaceParser
  _ <- queryStatementParser "set"
  _ <- whitespaceParser
  seperate whereConditionParser (optional whitespaceParser >> char ',' >> optional whitespaceParser)

----Lib2 stuff----
-- selectAllParser :: Parser ParsedStatement3
-- selectAllParser = do
--   _ <- queryStatementParser "select"
--   _ <- whitespaceParser
--   _ <- queryStatementParser "*"
--   _ <- whitespaceParser
--   _ <- queryStatementParser "from"
--   _ <- whitespaceParser
--   tableArray <- selectTablesParser
--   selectWhere <- optional whereParser
--   _ <- optional whitespaceParser
--   pure $ SelectAll tableArray selectWhere

selectNowParser :: Parser ParsedStatement3
selectNowParser = do
    _ <- queryStatementParser "select"
    _ <- whitespaceParser
    _ <- queryStatementParser "now"
    _ <- optional whitespaceParser
    _ <- queryStatementParser "("
    _ <- optional whitespaceParser
    _ <- queryStatementParser ")"
    pure SelectNow

-- selectStatementParser :: Parser ParsedStatement3
-- selectStatementParser = do
--     _ <- queryStatementParser "select"
--     _ <- whitespaceParser
--     specialSelect <- selectDataParser
--     _ <- whitespaceParser
--     _ <- queryStatementParser "from"
--     _ <- whitespaceParser
--     tableArray <- selectTablesParser
--     selectWhere <- optional whereParser
--     _ <- optional whitespaceParser
--     pure $ Lib4.Select specialSelect tableArray selectWhere

selectDataParser :: Parser SpecialSelect2
selectDataParser = tryParseAggregate <|>  tryParseColumn <|> tryParseColumnTable
  where
    tryParseAggregate = do
      nowf <- optional nowParser
      aggregateList <- seperate aggregateParser' (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      _ <- trashParser
      return $ SelectAggregate2 aggregateList nowf
    tryParseColumn = do
      nowf <- optional nowParser
      columnNames <- seperate columnNameParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      _ <- trashParser
      return $ SelectColumn2 columnNames nowf
    tryParseColumnTable = do
      nowf <- optional nowParser
      columnsWithTables <- seperate columnNameTableParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      _ <- trashParser
      return $ SelectedColumnsTables columnsWithTables nowf

aggregateParser' :: Parser Aggregate2
aggregateParser' = tryColumnTables <|> tryColumns
  where 
    tryColumns = do
      func <- aggregateFunctionParser
      _ <- optional whitespaceParser
      _ <- char '('
      _ <- optional whitespaceParser
      columnName <- columnNameParser''
      _ <- optional whitespaceParser
      _ <- char ')'
      return $ AggregateColumn (func, columnName)
    tryColumnTables = do
      func <- aggregateFunctionParser
      _ <- optional whitespaceParser
      _ <- char '('
      _ <- optional whitespaceParser
      columnName <- columnNameTableParser
      _ <- optional whitespaceParser
      _ <- char ')'
      return $ AggregateColumnTable (func, columnName)

nowParser :: Parser NowFunction
nowParser = do
  _ <- optional whitespaceParser
  _ <- queryStatementParser "now"
  _ <- optional whitespaceParser
  _ <- queryStatementParser "("
  _ <- optional whitespaceParser
  _ <- queryStatementParser ")"
  _ <- optional whitespaceParser
  _ <- queryStatementParser ","
  _ <- optional whitespaceParser
  pure Now

whereParser :: Parser WhereSelect
whereParser = do
  _ <- whitespaceParser
  _ <- queryStatementParser "where"
  _ <- whitespaceParser
  some whereAndExist
  where
    whereAndExist :: Parser Condition
    whereAndExist = do
      condition <- whereConditionParser
      _ <- optional (whitespaceParser >> andParser)
      pure condition

andParser :: Parser And
andParser = queryStatementParser "and" >> pure And

whereConditionParser :: Parser Condition
whereConditionParser = do
  _ <- optional whitespaceParser
  operand1 <- operandParser
  _ <- optional whitespaceParser
  operator <- operatorParser
  _ <- optional whitespaceParser
  operand2 <- operandParser
  return $ Condition operand1 operator operand2

operandParser :: Parser Operand
operandParser = (ConstantOperand <$> constantParser)
               <|> (ColumnTableOperand <$> columnNameTableParser)
               <|> (ColumnOperand <$> columnNameParser)

columnNameTableParser :: Parser (TableName, ColumnName)
columnNameTableParser = do
  table <- columnNameParser
  _ <- char '.'
  column <- columnNameParser
  return (table, column)

-------------------------------------------------------------------------------

selectTablesParser :: Parser TableArray
selectTablesParser = tryParseTable
  where
    tryParseTable = do
      tableNames <- seperate columnNameParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      return $ tableNames

showTablesParser :: Parser ParsedStatement3
showTablesParser = do
    _ <- queryStatementParser "show"
    _ <- whitespaceParser
    _ <- queryStatementParser "tables"
    _ <- optional whitespaceParser
    pure Lib4.ShowTables

-----------------------------------------------------------------------------------------------------------

showTableParser :: Parser ParsedStatement3
showTableParser = do
    _ <- queryStatementParser "show"
    _ <- whitespaceParser
    _ <- queryStatementParser "table"
    _ <- whitespaceParser
    table <- columnNameParser
    _ <- optional whitespaceParser
    pure $ Lib4.ShowTable table

columnListParser :: Parser [Column]
columnListParser = seperate columnAndTypeParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)

columnAndTypeParser :: Parser Column
columnAndTypeParser = do
    columnName <- columnNameParser
    _ <- whitespaceParser
    columnType <- columnNameParser >>= either (throwE . show) pure . columnTypeParser
    pure (Column columnName columnType)

columnTypeParser :: String -> Either Error ColumnType
columnTypeParser "int" = Right IntegerType
columnTypeParser "varchar" = Right StringType
columnTypeParser "bool" = Right BoolType
columnTypeParser other = Left $ "There is no such type as: " ++ other

--sitas gali ir neveikti:
parseSatisfy :: (Char -> Bool) -> Parser Char
parseSatisfy pr = EitherT $ state $ \input ->
  case input of
    (x:xs) -> if pr x then (Right x, xs) else (Left ("Unexpected character: " ++ [x]), input)
    [] -> (Left "Empty input", input)

--sitas gali ir neveikti:
queryStatementParser :: String -> Parser String
queryStatementParser stmt = do
  input <- lift get
  case take (length stmt) input of
    [] -> throwE "Expected ;"
    xs
      | map toLower xs == map toLower stmt -> do
        lift $ put $ drop (length xs) stmt
        return xs
      | otherwise -> throwE $ "Expected " ++ stmt ++ " or query contains unnecessary words"

whitespaceParser :: Parser String
whitespaceParser = do
  input <- lift get
  case span isSpace input of
    ("",_) -> throwE $ "Expected whitespace before: " ++ input
    (ws, remain) -> do
      lift $ put remain
      return ws

seperate :: Parser a -> Parser b -> Parser [a]
seperate p sep = do
    x <- p
    xs <- many (sep *> p)
    return (x:xs)

columnNameParser :: Parser ColumnName
columnNameParser = do
  input <- lift get
  case takeWhile (\x -> isAlphaNum x || x == '_') input of
    [] -> throwE "Empty input"
    xs -> do
      lift $ put $ drop (length xs) input
      return xs

char :: Char -> Parser Char
char c = do
  input <- lift get
  case input of
    [] -> throwE "Empty input"
    (x:xs) -> if x == c then do 
                            lift $ put xs 
                            return c
                        else throwE ("Expected " ++ [c])

trashParser :: Parser Trash 
trashParser = do
  input <- lift get
  case head input == ',' of 
    True -> throwE "Columns are not listed right"
    False -> case "from" `isPrefixOf` (dropWhiteSpaces input) of 
      True -> do
                lift $ put input
                return $ Trash ""
      False -> throwE "Columns are not listed right or aggregate functions and column names cannot be mixed"

constantParser :: Parser Value
constantParser = do
  input <- lift get
  case input == "" of 
    True -> throwE "The query does not end with a ;"
    False -> 
      let operand = getOperand input
          restQuery = drop (length operand) input
      in case head input == ';' of
          False -> case head operand == '\'' && last operand == '\'' of
            True -> do
                      lift $ put restQuery
                      return $ StringValue (init (tail operand))
            False -> case operand of
              "True" -> do
                          lift $ put restQuery
                          return $ BoolValue True
              "False" -> do
                          lift $ put restQuery
                          return $ BoolValue False
              "null" -> do
                          lift $ put restQuery
                          return $ NullValue
              _ ->  if isNumber operand
                    then do 
                            lift $ put restQuery
                            return $ IntegerValue $ stringToInt operand
                    else throwE "Operand is not valid"
          True -> throwE "The conditions are missing"


columnNameParser' :: Parser ColumnName
columnNameParser' = do
  input <- lift get
  (if areSpacesBetweenWords (fst (splitStatementAtParentheses input)) 
    then do
            lift $ put $ snd (splitStatementAtParentheses input) 
            return $ dropWhiteSpaces (fst (splitStatementAtParentheses input))
  else throwE "There is more than one column name in aggregation function")


columnNameParser'' :: Parser ColumnName
columnNameParser'' = do
  input <- lift get
  (if areSpacesBetweenWords (fst (splitStatementAtParentheses input)) 
    then do
            lift $ put $ snd (splitStatementAtParentheses input)
            return $ dropWhiteSpaces (fst (splitStatementAtParentheses input))
  else throwE "There is more than one column name in aggregation function")

operatorParser :: Parser Operator
operatorParser = 
  (queryStatementParser "=" >> pure IsEqualTo)
  <|> (queryStatementParser "!=" >> pure IsNotEqual)
  <|> (queryStatementParser "<" >> pure IsLessThan)
  <|> (queryStatementParser ">" >> pure IsGreaterThan)
  <|> (queryStatementParser "<=" >> pure IsLessOrEqual)
  <|> (queryStatementParser ">=" >> pure IsGreaterOrEqual)

aggregateFunctionParser :: Parser AggregateFunction
aggregateFunctionParser = sumParser <|> maxParser
  where
    sumParser = do
        _ <- queryStatementParser "sum"
        pure Sum
    maxParser = do
        _ <- queryStatementParser "max"
        pure Max
