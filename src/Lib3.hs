{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use <$>" #-}
{-# LANGUAGE OverloadedStrings #-}

module Lib3
  ( executeSql,
    parseStatement,
    Execution,
    ParsedStatement2,
    ExecutionAlgebra(..),
    deserializedContent
  )
where
import Lib2
import Lib1 (renderDataFrameAsTable, findTableByName, parseSelectAllStatement, checkTupleMatch, zipColumnsAndValues, checkRowSizes)
import Control.Monad.Free (Free (..), liftF)
import DataFrame as DF
import Data.Time ( UTCTime )
import Data.Either (fromRight)
import Text.ParserCombinators.ReadP (many1)
import GHC.Generics
import Data.Aeson --(decode, FromJSON)
import Control.Monad
import GHC.Base (VecElem(DoubleElemRep))
import qualified Data.ByteString.Lazy.Char8 as BS
import Text.Read (readMaybe)
 
type TableName = String
type FileContent = String
type DeserializedContent = (TableName, DataFrame)
type ErrorMessage = String
type TableArray =  [TableName]

data ExecutionAlgebra next
  = LoadFile TableName (Either ErrorMessage DeserializedContent -> next)
  | GetTime (UTCTime -> next)
  -- feel free to add more constructors heres 
  deriving Functor

type Execution = Free ExecutionAlgebra


------------------------------- data -----------------------------------

data FromJSONTable = FromJSONTable {
    deserializedtableName :: String,
    deserializedColumns :: [FromJSONColumn],
    deserializedRows :: [FromJSONRow]
} deriving (Show, Eq, Generic)

data FromJSONColumn = FromJSONColumn {
    deserializedName :: String,
    deserializedDataType :: String
} deriving (Show, Eq, Generic)

data FromJSONValue = FromJSONValue {
    deserializedValue :: String              --------------------------------------------------------susitvarkyt gavus info su situo
} deriving (Show, Eq, Generic)

data FromJSONRow = FromJSONRow {
    deserializedRow :: [FromJSONValue]
} deriving (Show, Eq, Generic)

----------------------------- instances ----------------------------------

instance FromJSON FromJSONColumn where
  parseJSON (Object v) =
    FromJSONColumn <$> v .: "Name"
                   <*> v .: "ColumnType"
  parseJSON _ = mzero

instance FromJSON FromJSONTable where
  parseJSON (Object v) =
    FromJSONTable <$> v .: "Table"
                  <*> v .: "Columns"
                  <*> v .: "Rows"
  parseJSON _ = mzero
 
instance FromJSON FromJSONValue where
  parseJSON (Object v) = 
    FromJSONValue <$> v .: "Value"
  parseJSON _ = mzero

instance FromJSON FromJSONRow where
  parseJSON (Object v) = 
    FromJSONRow <$> v .: "Row"
  parseJSON _ = mzero

------------------------------------------ a veiksi padliau? ---------------------------------------------------

deserializedContent :: FileContent -> Either ErrorMessage (TableName, DataFrame)
deserializedContent json = case (toTable json) of
    Just fromjsontable -> Right (toDataframe fromjsontable)
    Nothing -> Left "Failed to decode JSON"
  

toTable :: String -> Maybe FromJSONTable 
toTable json = decode $ BS.pack json

toDataframe :: FromJSONTable -> (TableName, DataFrame)
toDataframe table =
  (deserializedtableName table ,DataFrame
    (map (\col -> Column (deserializedName col) (toColumnType $ deserializedDataType col)) $ deserializedColumns table)
    (map (map convertToValue . deserializedRow) $ deserializedRows table))

toColumnType :: String -> ColumnType
toColumnType "IntegerType" = IntegerType
toColumnType "StringType"  = StringType
toColumnType "BoolType"    = BoolType
toColumnType _         = error "Unsupported data type"

convertToValue :: FromJSONValue -> DF.Value
convertToValue (FromJSONValue str) =
  case words str of
    ["IntegerValue", val] -> IntegerValue (read val)
    ["StringValue", val] -> StringValue val
    ["BoolValue", "True"] -> BoolValue True
    ["BoolValue", "False"] -> BoolValue False
    ["NullValue"] -> NullValue
    _ -> error "Invalid FromJSONValue format"

-- handleDecodingResult :: Maybe FromJSONTable -> IO ()  --------sitas siudena jauciu turetu but pertvarkytas pagal execute preikius, cia petro isminti, kaip sujungt
-- handleDecodingResult maybeTable =
--   case maybeTable of
--     Just table -> do
--       let dataframe = toDataframe table
--       putStrLn $ "Decoded Result: " ++ show (deserializedtableName table, dataframe)
--     Nothing    -> putStrLn "Failed to decode JSON"  -----------------------------------------------------------------------------------------------siudenos pabaiga


----------------------------------------------------------------------------------------------------------------

-- Keep the type, modify constructors
data ParsedStatement2 =
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
  | ShowTable {
    table :: TableName
   }
  |SelectAll {
    tables :: TableArray,
    selectWhere :: Maybe WhereSelect
   }
  |Select {
    selectQuery :: SpecialSelect,
    tables :: TableArray,
    selectWhere :: Maybe WhereSelect
  }
  | ShowTables { }
    deriving (Show, Eq)

data SelectedColumns = ColumnsSelected [ColumnName]
  deriving (Show, Eq)

type InsertedValues = [DF.Value]

loadFile :: TableName -> Execution (Either ErrorMessage DeserializedContent)
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = case parseStatement2 sql of
  Left err -> return $ Left err
  Right (ShowTable table) -> do
    content <- loadFile table
    case content of 
      Left err -> return $ Left err
      Right content2 -> return $ Right (snd content2)
  --Mildai:
  -- Right (SelectNow) -> do
  --   da <- getTime
  --   let df = todataframe da
  --   return $ Right df

-- createNowDataFrame :: UTCTime -> DataFrame
-- createNowDataFrame time = DataFrame [Column "Now" StringType] [] <-Stringas

-------------------------------some JSON shit------------------------------------

toJSONtable :: (TableName, (DataFrame)) -> String
toJSONtable table = "{\"Table\":\"" ++ show(fst(table)) ++ "\",\"Columns\":[" ++ show(toJSONColumns(toColumnList(snd(table)))) ++ "],\"Rows\":[" ++ show(toJSONRows(toRowList(snd(table)))) ++"]}"

toJSONColumn :: Column -> String
toJSONColumn column = "{\"Name\":\"" ++ show(getColumnName(column)) ++ ",\"ColumnType\":\"" ++ show(getType(column)) ++ "\"}"

toJSONRowValue :: DF.Value -> String
toJSONRowValue value = "{\"Value\":\"" ++ show(value) ++ "\"}"

---------------------------------------some get stuff----------------------------

toColumnList :: DataFrame -> [Column]
toColumnList (DataFrame col row)  = col

toRowList :: DataFrame -> [Row]
toRowList (DataFrame co row) = row

----------------------------------recursive stuff-------------------------------

toJSONColumns :: [Column] -> String
toJSONColumns (x:xs)
  |xs /= [] = toJSONColumn x ++ "," ++ toJSONColumns xs
  |otherwise = toJSONColumn x

toJSONRow :: Row -> String
--toJSONRow [] = []
toJSONRow (x:xs) 
  |xs /= [] = toJSONRowValue x ++ "," ++ toJSONRow xs
  |otherwise = toJSONRowValue x

toJSONRows :: [Row] -> String
toJSONRows (x:xs)
  |xs /= [] = "{\"Row\":[" ++ toJSONRow x ++ "]}," ++ toJSONRows xs
  |otherwise = "{\"Row\":[" ++ toJSONRow x ++ "]}" 

--------------------------------Files--------------------------------------------
-- --C:\Users\Gita\Documents\GitHub\fp-2023\db\flags.json
toFilePath :: TableName -> FilePath
toFilePath tableName = "db/" ++ show(tableName) ++ ".json" --".txt"

writeTableToFile :: (TableName, DataFrame) -> IO () 
writeTableToFile table = writeFile (toFilePath(fst(table))) (toJSONtable(table))

---------------------------------------------------------------------------------

parseStatement2 :: String -> Either ErrorMessage ParsedStatement2
parseStatement2 query = case runParser p query of
    Left err1 -> Left err1
    Right (query, rest) -> case query of
        Select _ _ _ -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
        ShowTable _ -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
        ShowTables -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query  
        SelectAll _ _ -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query  
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
        p :: Parser ParsedStatement2
        p = showTableParser
               <|> showTablesParser
               <|> selectStatementParser
               <|> selectAllParser
               <|> insertParser
               <|> updateParser
               <|> deleteParser
               <|> selectNowParser

selectNowParser :: Parser ParsedStatement2
selectNowParser = do
    _ <- queryStatementParser "select"
    _ <- whitespaceParser
    _ <- queryStatementParser "now"
    _ <- optional whitespaceParser
    _ <- queryStatementParser "("
    _ <- optional whitespaceParser
    _ <- queryStatementParser ")"
    pure SelectNow

insertParser :: Parser ParsedStatement2
insertParser = do
    _ <- queryStatementParser "insert"
    _ <- whitespaceParser
    _ <- queryStatementParser "into"
    _ <- whitespaceParser
    tableName <- columnNameParser
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

deleteParser :: Parser ParsedStatement2
deleteParser = do
    _ <- queryStatementParser "delete"
    _ <- whitespaceParser
    _ <- queryStatementParser "from"
    _ <- whitespaceParser
    tableName <- columnNameParser
    _ <- optional whitespaceParser
    _ <- queryStatementParser "where"
    conditions <- optional whereParser
    pure $ Delete tableName conditions

updateParser :: Parser ParsedStatement2
updateParser = do
    _ <- queryStatementParser "update"
    _ <- whitespaceParser
    tableName <- columnNameParser
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

----Lib2 stuff----
selectAllParser :: Parser ParsedStatement2
selectAllParser = do
  _ <- queryStatementParser "select"
  _ <- whitespaceParser
  _ <- queryStatementParser "*"
  _ <- whitespaceParser
  _ <- queryStatementParser "from"
  _ <- whitespaceParser
  tableArray <- selectTablesParser
  selectWhere <- optional whereParser
  _ <- optional whitespaceParser
  pure $ SelectAll tableArray selectWhere

selectStatementParser :: Parser ParsedStatement2
selectStatementParser = do
    _ <- queryStatementParser "select"
    _ <- whitespaceParser
    specialSelect <- selectDataParser
    _ <- whitespaceParser
    _ <- queryStatementParser "from"
    _ <- whitespaceParser
    tableArray <- selectTablesParser
    _ <- optional whitespaceParser
    selectWhere <- optional whereParser
    _ <- optional whitespaceParser
    pure $ Select specialSelect tableArray selectWhere

selectTablesParser :: Parser TableArray
selectTablesParser = tryParseTable
  where
    tryParseTable = do
      tableNames <- seperate columnNameParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      return $ tableNames

showTablesParser :: Parser ParsedStatement2
showTablesParser = do
    _ <- queryStatementParser "show"
    _ <- whitespaceParser
    _ <- queryStatementParser "tables"
    _ <- optional whitespaceParser
    pure ShowTables

-----------------------------------------------------------------------------------------------------------

showTableParser :: Parser ParsedStatement2
showTableParser = do
    _ <- queryStatementParser "show"
    _ <- whitespaceParser
    _ <- queryStatementParser "table"
    _ <- whitespaceParser
    table <- columnNameParser
    _ <- optional whitespaceParser
    pure $ ShowTable table
    