{-# LANGUAGE DeriveFunctor #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use <$>" #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..)
  )
where

import Lib2
import Lib1 (renderDataFrameAsTable, findTableByName, parseSelectAllStatement, checkTupleMatch, zipColumnsAndValues, checkRowSizes)
import Control.Monad.Free (Free (..), liftF)
import DataFrame
import Data.Time ( UTCTime )
import Data.Either (fromRight)
import Text.ParserCombinators.ReadP (many1)

 
type TableName = String
type FileContent = String
type ErrorMessage = String

data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)
  | GetTime (UTCTime -> next)
  -- feel free to add more constructors here
  deriving Functor

type Execution = Free ExecutionAlgebra

-- Keep the type, modify constructors
data ParsedStatementVol2 =
  SelectNow {}
  | Insert {
    table :: TableName,
    columns :: Maybe SelectedColumns,
    values :: InsertedValues
  }
  | Update {
    table :: TableName,
    selectUpdate :: Maybe WhereSelect,
    selectWhere :: Maybe WhereSelect
  }
  | Delete {
    table :: TableName,
    conditions :: Maybe WhereSelect
  }
    deriving (Show, Eq)


data SelectedColumns = ColumnsSelected [ColumnName]
  deriving (Show, Eq)

type InsertedValues = [Value]

loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = do
    let ps = fromRight (ShowTables) (parseStatement sql)
    let df = fromRight (DataFrame [] []) (executeStatement ps)
    return $ Right df

-- createNowDataFrame :: UTCTime -> DataFrame
-- createNowDataFrame time = DataFrame [Column "Now" StringType] (time)


parseStatementVol2 :: String -> Either ErrorMessage ParsedStatementVol2
parseStatementVol2 query = case runParser p query of
    Left err1 -> Left err1
    Right (query, rest) -> case query of
        Insert _ _ _ -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
        Update _ _ _-> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
        Delete _ _ -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
        SelectNow -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
    where
        p :: Parser ParsedStatementVol2
        p = insertParser
               <|> updateParser
               <|> deleteParser
               <|> selectNowParser

selectNowParser :: Parser ParsedStatementVol2
selectNowParser = do
    _ <- queryStatementParser "select"
    _ <- whitespaceParser
    _ <- queryStatementParser "now"
    _ <- optional whitespaceParser
    _ <- queryStatementParser "("
    _ <- optional whitespaceParser
    _ <- queryStatementParser ")"
    pure SelectNow

insertParser :: Parser ParsedStatementVol2
insertParser = do
    _ <- queryStatementParser "insert"
    _ <- whitespaceParser
    _ <- queryStatementParser "into"
    _ <- whitespaceParser
    tableName <- tableNameParser
    _ <- optional whitespaceParser
    _ <- queryStatementParser "("
    selectUpdate <- optional selectDataParsers
    _ <- queryStatementParser ")"
    _ <- queryStatementParser "values"
    _ <- optional whitespaceParser
    _ <- queryStatementParser "("
    values <- insertedValuesParser
    _ <- queryStatementParser ")"
    pure $ Insert tableName selectUpdate values

insertedValuesParser :: Parser InsertedValues
insertedValuesParser = do
  values <- sepBy constantParser (char ',' >> whitespaceParser)
  return values

sepBy :: Parser a -> Parser b -> Parser [a]
sepBy p sep = do
    x <- p
    xs <- many (sep *> p)
    return (x:xs)

deleteParser :: Parser ParsedStatementVol2
deleteParser = do
    _ <- queryStatementParser "delete"
    _ <- whitespaceParser
    _ <- queryStatementParser "from"
    _ <- whitespaceParser
    tableName <- tableNameParser
    _ <- optional whitespaceParser
    _ <- queryStatementParser "where"
    conditions <- optional whereParser
    pure $ Delete tableName conditions

updateParser :: Parser ParsedStatementVol2
updateParser = do
    _ <- queryStatementParser "update"
    _ <- whitespaceParser
    tableName <- tableNameParser
    _ <- whitespaceParser
    _ <- queryStatementParser "set"
    selectUpdated <- optional setParserWithComma
    _ <- whitespaceParser
    selectedWhere <- optional whereParser
    pure $ Update tableName selectUpdated selectedWhere

setParserWithComma :: Parser WhereSelect
setParserWithComma = do
  _ <- whitespaceParser
  some setCommaExist

  where
    setCommaExist :: Parser Condition
    setCommaExist = do
      condition <- whereConditionParser
      _ <- optional (queryStatementParser "," >> whitespaceParser)
      pure condition

selectDataParsers :: Parser SelectedColumns
selectDataParsers = tryParseColumn
  where
    tryParseColumn = do
      columnNames <- seperate columnNameParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      _ <- trashParser
      return $ ColumnsSelected columnNames